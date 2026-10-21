//! Group-basis aggregates whose source is another definition's target
//! (#1100's chained sources, #1096 decision 5, ADR-0002 "Multi-hop").
//!
//! A target is never captured: its writers stage its changes through the
//! target-mutation seam, and a target with a group-basis reader is on the
//! seam's image feed (`staging::target_mutations`, "The image feed"). Each
//! test here drives the pipeline by hand through `support/drain_driver.rs`
//! with the group-basis dev knob on, and none sleeps or polls for
//! convergence (#297). Every source starts empty and is written through
//! capture once every definition is live, except where a test says it builds
//! over rows.
//!
//! The first tests port the ledger's chained-aggregate tests
//! (`defs_aggregate_chained_single_column_group_key.rs`,
//! `defs_aggregate_chained_composite_group_key.rs`,
//! `defs_deleted_oneone_target_reduces_chained_aggregate.rs`) to the group
//! basis. The rest are decision 5's: a row that moves group upstream (#1043
//! F8), a rollup whose upstream group is emptied and refilled in one page, a
//! reader defined while an upstream writer is in flight, and an upstream
//! rebuild that rebuilds the reader. The last covers a ledger upstream fed
//! through a relationship, whose writes reach the reader through the seam
//! like any other writer's.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;

const SRC: &str = "public.src";
const SRC_DDL: &str = "create table public.src (id integer primary key, g integer, h integer, \
                       v integer); \
                       create index on public.src (g, h)";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

fn int8() -> ValueType {
    ValueType::Integer(IntWidth::Int8)
}

fn src_columns() -> Vec<(&'static str, ValueType)> {
    vec![("id", int4()), ("g", int4()), ("h", int4()), ("v", int4())]
}

/// `public.src (id, g, h, v)`, empty and captured, with `upstream` live over
/// it and `downstream` (over `upstream`'s target, whose columns are
/// `downstream_columns`) live after it, every aggregate on the group basis.
async fn start(
    upstream: &str,
    downstream: &str,
    downstream_columns: &[(&str, ValueType)],
    aggregates: usize,
) -> Driver {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(SRC_DDL, &src_columns(), &[upstream], &[SRC]).await;
    install(&d, downstream, downstream_columns).await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_eq!(
        d.rows("select status from transform_definitions order by id")
            .await,
        ["(live)", "(live)"]
    );
    assert_eq!(hidden_tables(&d).await, aggregates, "on the group basis");
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

/// How many group-basis hidden tables the catalog schema holds.
async fn hidden_tables(d: &Driver) -> usize {
    d.rows(&format!(
        "select 1 from pg_tables where schemaname = '{DEFAULT_SCHEMA}' \
         and tablename like 'agg\\_%'"
    ))
    .await
    .len()
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it, and everything it propagates, to quiescence.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    d.settle().await;
}

/// Asserts `actual` equals the oracle `expected`, both over the settled
/// pipeline.
async fn assert_equal(d: &mut Driver, actual: &str, expected: &str) {
    d.settle().await;
    assert_eq!(
        d.rows(actual).await,
        d.rows(expected).await,
        "{actual} (left) differs from the oracle (right)"
    );
}

// ---------------------------------------------- ported chained aggregates

const AGG_G: &str =
    "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";

fn agg_g_columns() -> Vec<(&'static str, ValueType)> {
    vec![("g", int4()), ("total", int8()), ("n", int8())]
}

/// An extinct upstream group removes the chained group it was the only
/// contributor to, and a shrunk one reduces it (#180), with a plain
/// single-column `GROUP BY` at both levels.
#[tokio::test]
async fn an_extinct_upstream_group_removes_its_rollup_group() {
    let mut d = start(
        AGG_G,
        "TRANSFORM roll FROM public.agg GROUP BY g SELECT SUM(total) AS total2",
        &agg_g_columns(),
        2,
    )
    .await;
    const ROLL: &str = "select g, total2 from public.roll order by g";
    const ORACLE: &str = "select g, sum(v) from public.src group by g order by g";
    step(
        &mut d,
        "insert into public.src values (1, 1, 0, 5), (2, 1, 0, 7), (3, 2, 0, 2), (4, 3, 0, 11)",
    )
    .await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    step(&mut d, "delete from public.src where id = 4").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    assert_eq!(
        d.rows("select g from public.roll where g = 3").await.len(),
        0
    );
    step(&mut d, "delete from public.src where id = 1").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
}

/// A coarser rollup over a composite upstream `GROUP BY`: an extinct
/// `(g, h)` group reduces its `g`'s total and its group count (#171, #180).
#[tokio::test]
async fn an_extinct_composite_upstream_group_reduces_its_coarser_rollup() {
    let mut d = start(
        "TRANSFORM agg FROM public.src GROUP BY g, h SELECT SUM(v) AS total, COUNT(*) AS n",
        "TRANSFORM roll FROM public.agg GROUP BY g \
         SELECT SUM(total) AS total2, COUNT(*) AS groups",
        &[
            ("g", int4()),
            ("h", int4()),
            ("total", int8()),
            ("n", int8()),
        ],
        2,
    )
    .await;
    const ROLL: &str = "select g, total2, groups from public.roll order by g";
    const ORACLE: &str = "select g, sum(v), count(distinct array[h]) from public.src \
                          group by g order by g";
    step(
        &mut d,
        "insert into public.src values \
         (1, 1, 1, 5), (2, 1, 1, 7), (3, 1, 2, 2), (4, 2, 1, 11), (5, 2, null, 3)",
    )
    .await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    step(&mut d, "delete from public.src where id = 3").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    // A NULL grouping part: the upstream group `(2, NULL)` goes extinct.
    step(&mut d, "delete from public.src where id = 5").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    step(&mut d, "delete from public.src where g = 2").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
}

const ONE: &str = "TRANSFORM m FROM public.src SELECT g AS g, v + v AS w";
const OVER_ONE: &str = "TRANSFORM agg FROM public.m GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(w) AS total, COUNT(*) AS n";
const OVER_ONE_ACTUAL: &str = "select g, total, n from public.agg order by g";
const OVER_ONE_ORACLE: &str =
    "select g, sum(v + v), count(*) from public.src group by g order by g";

fn one_columns() -> Vec<(&'static str, ValueType)> {
    vec![("id", int4()), ("g", int4()), ("w", int4())]
}

/// A deleted row of a 1-1 target reduces the aggregate over it, and removes
/// the group it was the last member of.
#[tokio::test]
async fn a_deleted_one_to_one_row_reduces_the_aggregate_over_it() {
    let mut d = start(ONE, OVER_ONE, &one_columns(), 1).await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 0, 5), (2, 1, 0, 7), (3, 2, 0, 2)",
    )
    .await;
    assert_equal(&mut d, OVER_ONE_ACTUAL, OVER_ONE_ORACLE).await;
    step(&mut d, "delete from public.src where id = 2").await;
    assert_equal(&mut d, OVER_ONE_ACTUAL, OVER_ONE_ORACLE).await;
    step(&mut d, "delete from public.src where id = 3").await;
    assert_equal(&mut d, OVER_ONE_ACTUAL, OVER_ONE_ORACLE).await;
}

// ------------------------------------------------------ decision 5's cases

/// The computed-measure pattern (#1043 F8): an upstream 1-1 row moves group,
/// so the aggregate over it decrements the group it left, which only the old
/// image names. A key-only `Recompute` would leave it counted there.
#[tokio::test]
async fn an_upstream_row_that_moves_group_leaves_its_old_group() {
    let mut d = start(ONE, OVER_ONE, &one_columns(), 1).await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 0, 5), (2, 1, 0, 7), (3, 2, 0, 2)",
    )
    .await;
    step(&mut d, "update public.src set g = 2, v = 9 where id = 1").await;
    assert_equal(&mut d, OVER_ONE_ACTUAL, OVER_ONE_ORACLE).await;
    // Two moves of one key in one page: the images chain through both.
    write(&d, "update public.src set g = 3 where id = 2").await;
    write(&d, "update public.src set g = 1, v = 1 where id = 2").await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_equal(&mut d, OVER_ONE_ACTUAL, OVER_ONE_ORACLE).await;
    // The upstream group-key change moves group 2's only member away.
    step(&mut d, "update public.src set g = 1 where id = 3").await;
    assert_equal(&mut d, OVER_ONE_ACTUAL, OVER_ONE_ORACLE).await;
}

/// A rollup grouped by an upstream value (each group's row count): one
/// upstream page moves both of group 1's rows out and two new rows in
/// (emptied and refilled), empties group 3 and grows group 2, so the rollup's
/// groups by `n` see each kind of upstream change, and group 1's row moves
/// between them only through its images.
#[tokio::test]
async fn a_rollup_follows_upstream_groups_emptied_and_refilled_in_one_page() {
    let mut d = start(
        AGG_G,
        "TRANSFORM roll FROM public.agg GROUP BY n WITHOUT SOURCE INDEX SELECT SUM(total) AS total, COUNT(*) AS groups",
        &agg_g_columns(),
        2,
    )
    .await;
    const ROLL: &str = "select n, total, groups from public.roll order by n";
    const ORACLE: &str = "select n, sum(total), count(*) from \
                          (select g, sum(v) as total, count(*) as n from public.src group by g) s \
                          group by n order by n";
    step(
        &mut d,
        "insert into public.src values (1, 1, 0, 10), (3, 1, 0, 7), (5, 2, 0, 1), (6, 3, 0, 4)",
    )
    .await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    write(
        &d,
        "update public.src set g = 2 where id in (1, 3, 6); \
         insert into public.src values (2, 1, 0, 5), (4, 1, 0, 6)",
    )
    .await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    // Emptied by one transaction and refilled by another, drained as one
    // page: the upstream group's row count passes through 0.
    write(&d, "delete from public.src where g = 1").await;
    write(&d, "insert into public.src values (7, 1, 0, 3)").await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
}

/// A reader defined while an upstream writer is in flight (ADR-0002,
/// "Multi-hop"): the upstream page resolved `m`'s feed before the reader
/// existed, so it stages nothing for it, and commits after the reader's
/// build started. The build's plan waits on its fence until that page has
/// ended, so it sees the page's row rather than missing it: the build
/// (#1101) counts it and the reader goes `live` with it.
#[tokio::test]
async fn a_reader_defined_while_an_upstream_writer_is_in_flight_does_not_miss_its_write() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(SRC_DDL, &src_columns(), &[ONE], &[SRC]).await;
    write(&d, "insert into public.src values (1, 1, 0, 5)").await;
    let batch = d.seal().await;
    let mut page = d
        .drain_frozen(batch, "writer", &[(PausePoint::BeforeCommit, "public.m")])
        .await;
    page.reached(PausePoint::BeforeCommit).await;

    install(&d, OVER_ONE, &one_columns()).await;
    trellis::intake::markers::discharge_registrations(d.pool())
        .await
        .expect("start the reader's build");
    assert_eq!(
        d.rows("select status, build from transform_definitions where target_table = 'public.agg'")
            .await,
        ["(backfilling,rederive)"],
        "the reader applies from its build's start"
    );
    // The plan waits for the writer, which began before its fence.
    assert!(run_plan(&d).await, "a plan job to claim");
    assert_eq!(
        d.rows("select done, fence_xid is not null from backfill_chunks where kind = 'plan'")
            .await,
        ["(f,t)"],
        "the plan defers on its fence"
    );

    d.release(&mut page, PausePoint::BeforeCommit).await;
    page.finish().await;
    d.ctl
        .batch_execute("update backfill_chunks set next_attempt_at = null")
        .await
        .expect("skip the plan's backoff");
    assert!(run_plan(&d).await, "the plan job again");
    assert_eq!(
        d.rows("select status from transform_definitions where target_table = 'public.agg'")
            .await,
        ["(live)"],
        "the build counted the writer's row"
    );
    assert_equal(&mut d, OVER_ONE_ACTUAL, OVER_ONE_ORACLE).await;
    assert_eq!(d.rows(OVER_ONE_ACTUAL).await, ["(1,10,1)"]);
}

/// A reader defined over an upstream that is already live with rows: the
/// build's plan waits on its fence, then one absolute write counts them.
#[tokio::test]
async fn a_reader_defined_over_a_seeded_upstream_builds_from_its_rows() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(SRC_DDL, &src_columns(), &[ONE], &[SRC]).await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 0, 5), (2, 1, 0, 6), (3, 2, 0, 7)",
    )
    .await;
    install(&d, OVER_ONE, &one_columns()).await;
    trellis::intake::markers::discharge_registrations(d.pool())
        .await
        .expect("start the reader's build");
    while run_plan(&d).await {
        d.ctl
            .batch_execute("update backfill_chunks set next_attempt_at = null")
            .await
            .expect("skip the plan's backoff");
    }
    assert_eq!(
        d.rows("select status from transform_definitions where target_table = 'public.agg'")
            .await,
        ["(live)"]
    );
    assert_equal(&mut d, OVER_ONE_ACTUAL, OVER_ONE_ORACLE).await;
    assert_eq!(d.rows(OVER_ONE_ACTUAL).await, ["(1,22,2)", "(2,14,1)"]);
}

/// Claims the one Re-derive plan job, if any is claimable, and runs it.
async fn run_plan(d: &Driver) -> bool {
    let claimed = chunk_queue::claim_chunks_of(
        &**d.pool().get().await.expect("pool"),
        "test",
        1,
        &[chunk_queue::KIND_PLAN],
    )
    .await
    .expect("claim the plan job");
    let Some(chunk) = claimed.first() else {
        return false;
    };
    let options = trellis::staging::build::WorkerOptions {
        chunk_rows: 1_000,
        drain_batch_cap: usize::MAX,
        heartbeat_interval: Duration::from_secs(5),
        reclaim_ttl: Duration::from_secs(60),
    };
    trellis::staging::build::run_claimed(d.pool(), chunk, "test", &options).await;
    true
}

/// An upstream rebuild that writes its target outside the seam (a resumed
/// relationship-fed aggregate, which only the direct build builds) rebuilds
/// a group-basis reader rather than parking it a catch-up, whose image-less
/// re-read the group basis can't apply (ADR-0002, "Multi-hop").
#[tokio::test]
async fn an_upstream_rebuild_outside_the_seam_rebuilds_the_reader() {
    trellis::dev::staging::set_group_basis(true);
    let mut columns = src_columns();
    columns.push(("x", int4()));
    let mut d = Driver::start_with_relationships(
        "create table public.par (id integer primary key, x integer); \
         create table public.src (id integer primary key, g integer, h integer, v integer); \
         create index on public.src (g, h)",
        &columns,
        &["RELATIONSHIP parent FROM src.h TO par.id"],
        &["TRANSFORM up FROM public.src GROUP BY g SELECT SUM(parent.x) AS total, COUNT(*) AS n"],
        &["public.par", SRC],
    )
    .await;
    install(
        &d,
        "TRANSFORM roll FROM public.up GROUP BY n WITHOUT SOURCE INDEX SELECT COUNT(*) AS groups",
        &[("g", int4()), ("total", int8()), ("n", int8())],
    )
    .await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    let status = |table: &str| {
        format!("select status, build from transform_definitions where target_table = '{table}'")
    };
    assert_eq!(d.rows(&status("public.roll")).await, ["(live,)"]);
    assert_eq!(
        hidden_tables(&d).await,
        1,
        "only the reader is on the group basis"
    );
    step(
        &mut d,
        "insert into public.par values (1, 100); \
         insert into public.src values (1, 1, 1, 5), (2, 1, 1, 7), (3, 2, null, 2)",
    )
    .await;
    assert_equal(
        &mut d,
        "select n, groups from public.roll order by n",
        "select n, count(*) from (select count(*) as n from public.src group by g) s \
         group by n order by n",
    )
    .await;

    // Pause and resume the upstream: its rebuild is a direct build, whose
    // writes bypass the seam.
    d.ctl
        .batch_execute(
            "update transform_definitions set status = 'paused' where target_table = 'public.up'",
        )
        .await
        .expect("pause the upstream");
    trellis::staging::quarantine::resume_transform(d.pool(), "up")
        .await
        .expect("resume the upstream");
    trellis::intake::markers::discharge_registrations(d.pool())
        .await
        .expect("dispatch the upstream's rebuild");
    assert_eq!(
        d.rows("select kind from backfill_chunks where not done")
            .await,
        ["(direct)"],
        "the upstream is rebuilt by the direct build"
    );
    loop {
        let claimed =
            chunk_queue::claim_chunks(&**d.pool().get().await.expect("pool"), "test", 100)
                .await
                .expect("claim the upstream's build");
        if claimed.is_empty() {
            break;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                d.pool(),
                chunk,
                "test",
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("run the upstream's build");
            chunk_queue::finish_chunk(d.pool(), chunk, "test")
                .await
                .expect("finish the upstream's build");
        }
    }
    assert_eq!(
        d.rows(&status("public.up")).await[0].split(',').next(),
        Some("(catching_up"),
        "the upstream's build finished"
    );
    assert_eq!(
        d.rows(&status("public.roll")).await,
        ["(backfilling,rederive)"],
        "the reader is rebuilt, not caught up"
    );
    assert_eq!(
        d.rows(
            "select build_seg is not null from transform_definitions \
             where target_table = 'public.roll'"
        )
        .await,
        ["(t)"],
        "the rebuild records the segment active at its commit"
    );
    assert_eq!(
        d.rows(
            "select bc.kind from backfill_chunks bc join transform_definitions d \
             on d.id = bc.definition_id where d.target_table = 'public.roll' and not bc.done \
             order by bc.kind"
        )
        .await,
        ["(sweep)"],
        "a group-basis rebuild is its sweep job alone (#1101)"
    );

    // The rebuild replaces the reader's groups from the upstream's target
    // as the upstream's build left it, and the reader stays exact as the
    // upstream catches up and is written again.
    trellis::staging::build::settle_builds(d.pool()).await;
    assert_eq!(d.rows(&status("public.roll")).await, ["(live,)"]);
    trellis::intake::markers::settle_registrations(d.pool()).await;
    let roll = "select n, groups from public.roll order by n";
    let oracle = "select n, count(*) from (select count(*) as n from public.src group by g) s \
                  group by n order by n";
    assert_equal(&mut d, roll, oracle).await;
    step(
        &mut d,
        "insert into public.src values (4, 2, null, 1), (5, 3, null, 1)",
    )
    .await;
    assert_equal(&mut d, roll, oracle).await;
}

/// A rebuild of a group-basis upstream reaches its group-basis reader
/// through the seam (#1101): each visible row the rebuild changes is staged
/// with the row the target held as its prior image, so the reader moves
/// each group between its rollup groups exactly once. Here the rebuild's
/// snapshot sees an undrained insert and delete, which it folds into
/// groups 2 and 3, and their own page is then refused upstream.
#[tokio::test]
async fn an_upstream_rebuild_reaches_its_reader_through_the_seam() {
    let mut d = start(
        AGG_G,
        "TRANSFORM roll FROM public.agg GROUP BY n WITHOUT SOURCE INDEX SELECT COUNT(*) AS groups, SUM(total) AS t",
        &agg_g_columns(),
        2,
    )
    .await;
    const ROLL: &str = "select n, groups, t from public.roll order by n";
    const ORACLE: &str = "select n, count(*), sum(total) from ( \
                              select count(*) as n, sum(v) as total from public.src group by g \
                          ) s group by n order by n";
    step(
        &mut d,
        "insert into public.src values (1, 1, 0, 5), (2, 1, 0, 7), (3, 2, 0, 2), (4, 3, 0, 11)",
    )
    .await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    write(
        &d,
        "insert into public.src values (5, 2, 0, 1); delete from public.src where id = 4",
    )
    .await;
    let undrained = d.seal().await;
    let trellis = trellis::Trellis::connect(
        trellis::config::Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    trellis
        .request_backfill("src")
        .await
        .expect("request a rebuild");
    trellis::staging::build::settle_builds(d.pool()).await;
    assert_eq!(
        d.rows("select status from transform_definitions order by id")
            .await,
        ["(live)", "(live)"]
    );
    d.drain(undrained, "undrained").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    step(&mut d, "insert into public.src values (6, 3, 0, 4)").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
}

/// A relationship-fed upstream (its totals follow a parent's value, which
/// reaches it through the relationship rather than its own source's capture)
/// still writes its target through the seam, so a group-basis rollup grouped
/// by those totals moves each upstream group between its groups.
#[tokio::test]
async fn a_rollup_over_a_relationship_fed_upstream_follows_parent_changes() {
    trellis::dev::staging::set_group_basis(true);
    let mut columns = src_columns();
    columns.push(("x", int4()));
    let mut d = Driver::start_with_relationships(
        "create table public.par (id integer primary key, x integer); \
         create table public.src (id integer primary key, g integer, h integer, v integer); \
         create index on public.src (g, h)",
        &columns,
        &["RELATIONSHIP parent FROM src.h TO par.id"],
        &["TRANSFORM up FROM public.src GROUP BY g SELECT SUM(parent.x) AS total, COUNT(*) AS n"],
        &["public.par", SRC],
    )
    .await;
    install(
        &d,
        "TRANSFORM roll FROM public.up GROUP BY total WITHOUT SOURCE INDEX SELECT SUM(n) AS rows, COUNT(*) AS groups",
        &[("g", int4()), ("total", int8()), ("n", int8())],
    )
    .await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    const ROLL: &str = "select total, rows, groups from public.roll order by total";
    const ORACLE: &str = "select total, sum(n), count(*) from \
                          (select s.g, sum(p.x) as total, count(*) as n from public.src s \
                           left join public.par p on p.id = s.h group by s.g) t \
                          group by total order by total";
    step(
        &mut d,
        "insert into public.par values (1, 100), (2, 10); \
         insert into public.src values (1, 1, 1, 0), (2, 2, 2, 0), (3, 3, 2, 0)",
    )
    .await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    step(&mut d, "update public.par set x = 50 where id = 2").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
    step(&mut d, "update public.par set x = 100 where id = 2").await;
    assert_equal(&mut d, ROLL, ORACLE).await;
}

// ------------------------------------- 1-1 readers of a nullable key (#1141)

/// A 1-1 reader of a group-basis aggregate holds the groups whose key has a
/// `NULL` part, as the aggregate does: its key is the aggregate's `UNIQUE
/// NULLS NOT DISTINCT` one (#1141). A row moves into, within and out of
/// such a group.
#[tokio::test]
async fn a_one_to_one_reader_holds_the_null_keyed_groups() {
    let mut d = start(
        "TRANSFORM agg FROM public.src GROUP BY g, h SELECT COUNT(*) AS n, SUM(v) AS t",
        "TRANSFORM a2 FROM public.agg SELECT n AS n, t + 1 AS t1",
        &[("g", int4()), ("h", int4()), ("n", int8()), ("t", int8())],
        1,
    )
    .await;
    const A2: &str = "select g, h, n, t1 from public.a2 order by g, h";
    const ORACLE: &str =
        "select g, h, count(*), sum(v) + 1 from public.src group by g, h order by g, h";
    for sql in [
        "insert into public.src values (1, 1, null, 1), (2, 1, 2, 1)",
        "insert into public.src values (3, 1, null, 3), (4, null, null, 4)",
        "update public.src set h = null where id = 2",
        "update public.src set v = 10 where id = 1",
        "update public.src set h = 5 where id = 3",
        "delete from public.src where h is null",
    ] {
        step(&mut d, sql).await;
        assert_equal(&mut d, A2, ORACLE).await;
    }
    step(&mut d, "insert into public.src values (5, null, null, 5)").await;
    assert_equal(&mut d, A2, ORACLE).await;
    assert_eq!(
        d.rows("select n from public.a2 where g is null and h is null")
            .await,
        ["(1)"]
    );
}

/// A 1-1 reader of a `JOIN` target, whose `GROUP BY` value can be `NULL`,
/// holds the `NULL` group as the `JOIN` target does (#1141).
#[tokio::test]
async fn a_one_to_one_reader_of_a_join_target_holds_the_null_group() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(
        "create table public.a (id integer primary key, ga integer, k integer, x integer); \
         create table public.b (id integer primary key, k integer, y integer); \
         create index on public.a (k); create index on public.b (k)",
        &[("id", int4()), ("ga", int4()), ("k", int4()), ("x", int4())],
        &[
            "TRANSFORM j FROM public.a JOIN public.b ON a.k = b.k GROUP BY a.ga \
           SELECT a.ga AS ga, COUNT(*) AS n, SUM(b.y) AS sy",
        ],
        &["public.a", "public.b"],
    )
    .await;
    install(
        &d,
        "TRANSFORM jr FROM public.j SELECT n AS n, sy + 1 AS sy1",
        &[("ga", int4()), ("n", int8()), ("sy", int8())],
    )
    .await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_eq!(
        d.rows("select status from transform_definitions order by id")
            .await,
        ["(live)", "(live)"]
    );
    const JR: &str = "select ga, n, sy1 from public.jr order by ga";
    const ORACLE: &str = "select a.ga, count(*), sum(b.y) + 1 from public.a join public.b \
                          on a.k = b.k group by a.ga order by a.ga";
    for sql in [
        "insert into public.b values (1, 1, 10), (2, 2, 20)",
        "insert into public.a values (1, null, 1, 0), (2, 1, 2, 0), (3, null, 2, 0)",
        "update public.a set ga = 1 where id = 3",
        "update public.a set ga = null where id = 2",
        "delete from public.b where id = 1",
        "delete from public.a where ga is null",
    ] {
        step(&mut d, sql).await;
        assert_equal(&mut d, JR, ORACLE).await;
    }
    step(&mut d, "insert into public.a values (4, null, 2, 0)").await;
    assert_equal(&mut d, JR, ORACLE).await;
    assert_eq!(
        d.rows("select n from public.jr where ga is null").await,
        ["(1)"]
    );
}

/// An aggregate over a 1-1 reader of a nullable key, grouped by a field:
/// a `NULL`-keyed row that leaves a group (its count changes, or its group
/// goes extinct upstream) is found in the group it left through its prior
/// image, which the 1-1 apply captures for a `NULL`-keyed row as for any
/// other (#1141).
#[tokio::test]
async fn a_rollup_over_a_one_to_one_reader_follows_its_null_keyed_rows() {
    let mut d = start(
        "TRANSFORM agg FROM public.src GROUP BY g, h SELECT COUNT(*) AS n",
        "TRANSFORM a2 FROM public.agg SELECT n AS n",
        &[("g", int4()), ("h", int4()), ("n", int8())],
        1,
    )
    .await;
    install(
        &d,
        "TRANSFORM roll FROM public.a2 GROUP BY n WITHOUT SOURCE INDEX SELECT COUNT(*) AS groups",
        &[("g", int4()), ("h", int4()), ("n", int8())],
    )
    .await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_eq!(
        d.rows("select status from transform_definitions order by id")
            .await,
        ["(live)", "(live)", "(live)"]
    );
    const ROLL: &str = "select n, groups from public.roll order by n";
    const ORACLE: &str = "select n, count(*) from (select count(*) as n from public.src \
                          group by g, h) s group by n order by n";
    for sql in [
        "insert into public.src values (1, 1, null, 1), (2, null, null, 1), (3, 1, 1, 1)",
        "insert into public.src values (4, 1, null, 1), (5, null, null, 1)",
        "insert into public.src values (6, null, null, 1)",
        "delete from public.src where id = 4",
        "delete from public.src where g is null and h is null",
        "update public.src set h = 2 where id = 1",
    ] {
        step(&mut d, sql).await;
        assert_equal(&mut d, ROLL, ORACLE).await;
    }
}
