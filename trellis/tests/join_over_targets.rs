//! `JOIN` transforms whose sides are other definitions' targets (#1108,
//! #1096 decision 5, ADR-0019 "A side may be another transform's target").
//!
//! A target is never captured: its writers stage its changes through the
//! target-mutation seam, and a target a `JOIN` reads is on the seam's image
//! feed (`staging::target_mutations`, "The image feed"), so each side
//! consumes exact old and new images in write-token order, as it consumes
//! captured ones. Here the `FROM` side is a 1-1 target (the computed-measure
//! pattern) and the joined side a group-basis aggregate target (a rollup),
//! each over a captured table.
//!
//! Each test drives the pipeline by hand through `support/drain_driver.rs`
//! with the group-basis dev knob on, and none sleeps or polls for
//! convergence (#297). After each step the `JOIN` target equals its query
//! written over the captured tables, so the upstreams are checked through
//! it.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;

const DDL: &str = "create table public.fa (id integer primary key, g integer, k integer, \
                       v integer); \
                   create table public.db (id integer primary key, k integer, gb integer, \
                       y integer); \
                   create index on public.db (k, gb)";

/// The `FROM` side's upstream: a 1-1 target with a computed measure.
const A1: &str = "TRANSFORM a1 FROM public.fa SELECT g AS g, k AS k, v + v AS w";
/// The joined side's upstream: a group-basis aggregate target.
const B1: &str = "TRANSFORM b1 FROM public.db GROUP BY k, gb SELECT SUM(y) AS ys, COUNT(*) AS nb";
/// The `JOIN` over both targets.
const J: &str = "TRANSFORM j FROM public.a1 LEFT JOIN public.b1 ON a1.k = b1.k \
                 GROUP BY a1.g, b1.gb WITHOUT SOURCE INDEX \
                 SELECT a1.g AS g, b1.gb AS gb, COUNT(*) AS n, SUM(a1.w) AS sw, \
                 SUM(b1.ys) AS sy, COUNT(b1.nb) AS cb";

const ACTUAL: &str = "select g, gb, n, sw, sy, cb from public.j order by g, gb";
/// `J` written over the captured tables.
const ORACLE: &str = "select a.g, b.gb, count(*), sum(a.v + a.v), sum(b.ys), count(b.nb) \
                      from public.fa a left join ( \
                          select k, gb, sum(y) as ys, count(*) as nb from public.db \
                          group by k, gb \
                      ) b on a.k = b.k \
                      group by a.g, b.gb order by a.g, b.gb";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

fn columns() -> Vec<(&'static str, ValueType)> {
    vec![
        ("id", int4()),
        ("g", int4()),
        ("k", int4()),
        ("v", int4()),
        ("gb", int4()),
        ("y", int4()),
    ]
}

/// The upstreams live over the empty captured tables, the `JOIN` not yet
/// defined.
async fn upstreams() -> Driver {
    trellis::dev::staging::set_group_basis(true);
    Driver::start(DDL, &columns(), &[A1, B1], &["public.fa", "public.db"]).await
}

/// [`upstreams`], with `J` defined over them and live.
async fn start() -> Driver {
    let mut d = upstreams().await;
    define_join(&d).await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_eq!(statuses(&d).await, ["(a1,live)", "(b1,live)", "(j,live)"]);
    d
}

async fn define_join(d: &Driver) {
    // The `FROM` side's columns: `a1`'s.
    let columns: HashMap<String, ValueType> =
        [("id", int4()), ("g", int4()), ("k", int4()), ("w", int4())]
            .into_iter()
            .map(|(name, ty)| (name.to_string(), ty))
            .collect();
    trellis::defs::install_definition(d.pool(), J, &columns, "public")
        .await
        .expect("define the JOIN");
}

async fn statuses(d: &Driver) -> Vec<String> {
    d.rows(
        "select substr(target_table, 8), status::text from transform_definitions \
         order by target_table",
    )
    .await
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it, and everything it propagates, to quiescence.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    d.settle().await;
}

/// Asserts `J` equals its query over the captured tables, the pipeline
/// settled.
async fn assert_exact(d: &mut Driver, when: &str) {
    d.settle().await;
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(ORACLE).await,
        "{when}: the JOIN target (left) differs from its query (right)"
    );
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

/// Runs every build to the end and the go-live catch-ups they park, then
/// drains.
async fn settle_all(d: &mut Driver) {
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
}

const SEED: &str = "insert into public.fa values \
                        (1, 1, 10, 1), (2, 1, 20, 2), (3, 2, 10, 3), (4, 2, null, 4), \
                        (5, null, 30, 5); \
                    insert into public.db values \
                        (1, 10, 1, 100), (2, 10, 1, 200), (3, 10, 2, 300), (4, 20, 1, 400), \
                        (5, null, 3, 500), (6, 40, 1, 600)";

/// The tables a chain reads are each definition's source and `JOIN` table:
/// both upstream targets are read tables (the set capture records, the
/// discharge's enumeration and status read), while only the captured
/// sources are captured, since a target is fed through the seam.
#[tokio::test]
async fn a_join_over_two_targets_reads_every_table_of_its_chain() {
    let d = start().await;
    let mut read = trellis::defs::all_source_tables(d.pool())
        .await
        .expect("read tables");
    read.sort();
    assert_eq!(read, ["public.a1", "public.b1", "public.db", "public.fa"]);
    let mut captured = trellis::defs::tables_to_capture(d.pool())
        .await
        .expect("captured tables");
    captured.sort();
    assert_eq!(captured, ["public.db", "public.fa"]);
}

/// Writes to each upstream's source reach the `JOIN` through the seam's
/// images: a row that moves `P` cell or `M` cell upstream leaves the old
/// one, which only its old image names, and an upstream group that goes
/// extinct moves its key's `FROM` rows into the unmatched group.
#[tokio::test]
async fn writes_to_both_upstreams_reach_the_join() {
    let mut d = start().await;
    step(&mut d, SEED).await;
    assert_exact(&mut d, "seeded").await;
    // The `FROM` side: a key change, a group change, a measure change.
    step(&mut d, "update public.fa set k = 20 where id = 1").await;
    assert_exact(&mut d, "a FROM row changed key").await;
    step(&mut d, "update public.fa set g = 3, v = 9 where id = 3").await;
    assert_exact(&mut d, "a FROM row changed group").await;
    // The joined side: an upstream row moves `(10, 1)` to `(10, 2)`, then
    // `(20, 1)` goes extinct, so key 20 is unmatched.
    step(&mut d, "update public.db set gb = 2 where id = 1").await;
    assert_exact(&mut d, "an upstream group shrank").await;
    step(&mut d, "delete from public.db where id = 4").await;
    assert_exact(&mut d, "an upstream group went extinct").await;
    // Both sides, several writes of one key, drained as one page: the
    // images chain through each upstream write in write-token order.
    write(&d, "update public.fa set k = 40 where id = 2").await;
    write(&d, "update public.fa set k = 10, g = 1 where id = 2").await;
    write(&d, "insert into public.db values (7, 20, 5, 1)").await;
    write(&d, "update public.db set k = 30 where id = 7").await;
    write(&d, "delete from public.fa where id = 5").await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_exact(&mut d, "a mixed page").await;
    // A `NULL` key upstream joins nothing on either side.
    step(
        &mut d,
        "update public.fa set k = null where id = 3; update public.db set k = null where id = 2",
    )
    .await;
    assert_exact(&mut d, "NULL keys").await;
}

/// A `TRUNCATE` of an upstream's source clears that upstream's target row by
/// row through the seam, so each side sees deletes, never a truncate of its
/// own; the `JOIN` follows, and refills.
#[tokio::test]
async fn upstream_truncates_reach_the_join() {
    let mut d = start().await;
    step(&mut d, SEED).await;
    step(&mut d, "truncate public.db").await;
    assert_exact(&mut d, "the joined side's source truncated").await;
    step(
        &mut d,
        "insert into public.db values (1, 10, 1, 7), (2, 20, 2, 8)",
    )
    .await;
    assert_exact(&mut d, "the joined side refilled").await;
    step(&mut d, "truncate public.fa").await;
    assert_exact(&mut d, "the FROM side's source truncated").await;
    // Truncated and refilled before one drain.
    write(
        &d,
        "insert into public.fa values (1, 1, 10, 1), (2, 2, 20, 2)",
    )
    .await;
    write(&d, "truncate public.fa").await;
    write(&d, "insert into public.fa values (3, 1, 20, 3)").await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_exact(&mut d, "truncated and refilled in one page").await;
}

/// A `JOIN` defined over upstreams that already hold rows builds from them
/// once its plan's seam fence settles.
#[tokio::test]
async fn a_join_defined_over_seeded_upstreams_builds_from_their_rows() {
    let mut d = upstreams().await;
    step(&mut d, SEED).await;
    define_join(&d).await;
    trellis::intake::markers::discharge_registrations(d.pool())
        .await
        .expect("start the JOIN's build");
    while run_plan(&d).await {
        d.ctl
            .batch_execute("update backfill_chunks set next_attempt_at = null")
            .await
            .expect("skip the plan's backoff");
    }
    assert_eq!(statuses(&d).await, ["(a1,live)", "(b1,live)", "(j,live)"]);
    assert_exact(&mut d, "built").await;
    step(&mut d, "update public.db set gb = 9 where k = 10").await;
    assert_exact(&mut d, "written after the build").await;
}

/// A `JOIN` defined while an upstream writer is in flight (#1096 decision
/// 5): the upstream page resolved its target's feed before the `JOIN`
/// existed, so it stages nothing for it, and commits after the `JOIN`'s
/// build started. The plan waits on its seam fence until that page has
/// ended, so the build counts the page's row rather than missing it. One
/// test per side.
#[tokio::test]
async fn a_join_defined_while_an_upstream_writer_is_in_flight_does_not_miss_its_write() {
    for (upstream, sql) in [
        ("public.a1", "insert into public.fa values (1, 1, 10, 5)"),
        ("public.b1", "insert into public.db values (1, 10, 1, 5)"),
    ] {
        let mut d = upstreams().await;
        step(
            &mut d,
            "insert into public.fa values (9, 2, 10, 1); insert into public.db values (9, 10, 2, 1)",
        )
        .await;
        write(&d, sql).await;
        let batch = d.seal().await;
        let mut page = d
            .drain_frozen(batch, "writer", &[(PausePoint::BeforeCommit, upstream)])
            .await;
        page.reached(PausePoint::BeforeCommit).await;

        define_join(&d).await;
        trellis::intake::markers::discharge_registrations(d.pool())
            .await
            .expect("start the JOIN's build");
        assert!(run_plan(&d).await, "{upstream}: a plan job to claim");
        assert_eq!(
            d.rows("select done, fence_xid is not null from backfill_chunks where kind = 'plan'")
                .await,
            ["(f,t)"],
            "{upstream}: the plan defers on its fence"
        );

        d.release(&mut page, PausePoint::BeforeCommit).await;
        page.finish().await;
        d.ctl
            .batch_execute("update backfill_chunks set next_attempt_at = null")
            .await
            .expect("skip the plan's backoff");
        assert!(run_plan(&d).await, "{upstream}: the plan job again");
        assert_eq!(
            statuses(&d).await,
            ["(a1,live)", "(b1,live)", "(j,live)"],
            "{upstream}"
        );
        assert_exact(&mut d, upstream).await;
    }
}

/// As [`a_join_defined_while_an_upstream_writer_is_in_flight_does_not_miss_its_write`],
/// for a `JOIN` whose `FROM` side is a captured table and whose joined table
/// alone is a target: the plan waits on the seam fence for the joined table
/// too.
#[tokio::test]
async fn a_join_whose_joined_table_alone_is_a_target_waits_for_an_upstream_writer() {
    const JC: &str = "TRANSFORM jc FROM public.fa LEFT JOIN public.b1 ON fa.k = b1.k \
                      GROUP BY fa.g, b1.gb WITHOUT SOURCE INDEX \
                      SELECT fa.g AS g, b1.gb AS gb, COUNT(*) AS n, SUM(b1.ys) AS sy";
    const JC_ACTUAL: &str = "select g, gb, n, sy from public.jc order by g, gb";
    const JC_ORACLE: &str = "select a.g, b.gb, count(*), sum(b.ys) from public.fa a \
                             left join (select k, gb, sum(y) as ys from public.db \
                                        group by k, gb) b on a.k = b.k \
                             group by a.g, b.gb order by a.g, b.gb";
    let mut d = upstreams().await;
    step(
        &mut d,
        "insert into public.fa values (9, 2, 10, 1); insert into public.db values (9, 10, 2, 1)",
    )
    .await;
    write(&d, "insert into public.db values (1, 10, 1, 5)").await;
    let batch = d.seal().await;
    let mut page = d
        .drain_frozen(batch, "writer", &[(PausePoint::BeforeCommit, "public.b1")])
        .await;
    page.reached(PausePoint::BeforeCommit).await;

    let columns: HashMap<String, ValueType> =
        [("id", int4()), ("g", int4()), ("k", int4()), ("v", int4())]
            .into_iter()
            .map(|(name, ty)| (name.to_string(), ty))
            .collect();
    trellis::defs::install_definition(d.pool(), JC, &columns, "public")
        .await
        .expect("define the JOIN");
    trellis::intake::markers::discharge_registrations(d.pool())
        .await
        .expect("start the JOIN's build");
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
    assert_eq!(statuses(&d).await, ["(a1,live)", "(b1,live)", "(jc,live)"]);
    d.settle().await;
    assert_eq!(d.rows(JC_ACTUAL).await, d.rows(JC_ORACLE).await);
}

/// A rebuild in place of either upstream (`request_backfill` of its source)
/// writes through the seam, so it reaches the `JOIN` as images, folding in
/// changes its snapshot saw before they drained, and the `JOIN` is not
/// rebuilt.
#[tokio::test]
async fn an_upstream_rebuild_in_place_reaches_the_join_through_the_seam() {
    let mut d = start().await;
    step(&mut d, SEED).await;
    let trellis = facade(&d).await;
    for (table, sql) in [
        (
            "db",
            "insert into public.db values (8, 20, 7, 1); delete from public.db where id = 3",
        ),
        (
            "fa",
            "update public.fa set k = 40 where id = 1; insert into public.fa values (8, 7, 20, 1)",
        ),
    ] {
        write(&d, sql).await;
        let undrained = d.seal().await;
        trellis
            .request_backfill(table)
            .await
            .expect("request a rebuild");
        trellis::staging::build::settle_builds(d.pool()).await;
        assert_eq!(statuses(&d).await, ["(a1,live)", "(b1,live)", "(j,live)"]);
        d.drain(undrained, "undrained").await;
        assert_exact(&mut d, table).await;
    }
    step(&mut d, "insert into public.db values (9, 40, 1, 2)").await;
    assert_exact(&mut d, "written after").await;
}

/// A `RESUME` of either upstream reaches the `JOIN`, which is exact with
/// writes before, during and after. The 1-1 upstream's Re-derive build
/// writes its rows through the seam, as images of the rows its target held.
/// The group-basis upstream's build starts from empty partials while its
/// target keeps the earlier rows, so the `JOIN` reading it is rebuilt, once,
/// when that build commits (#1287).
#[tokio::test]
async fn an_upstream_resume_reaches_the_join() {
    let mut d = start().await;
    step(&mut d, SEED).await;
    for upstream in ["a1", "b1"] {
        if upstream == "b1" {
            // A hand edit of a visible row no write reaches: only a rebuild
            // of the `JOIN` compares its visible rows with `T` and repairs
            // it.
            d.ctl
                .batch_execute("update public.j set n = 99 where g is null and gb is null")
                .await
                .expect("plant a divergence");
        }
        d.ctl
            .batch_execute(&format!(
                "update transform_definitions set status = 'paused' \
                 where target_table = 'public.{upstream}'"
            ))
            .await
            .expect("pause the upstream");
        // Rows deleted while the upstream is paused leave its target only
        // through its rebuild, and the `JOIN` must follow.
        let (fa_gone, db_gone) = if upstream == "a1" { (2, 3) } else { (5, 2) };
        step(
            &mut d,
            &format!(
                "update public.fa set g = coalesce(g, 0) + 1 where id in (1, 4); \
                 update public.db set y = y + 1 where id in (1, 6); \
                 delete from public.fa where id = {fa_gone}; \
                 delete from public.db where id = {db_gone}"
            ),
        )
        .await;
        trellis::staging::quarantine::resume_transform(d.pool(), upstream)
            .await
            .expect("resume the upstream");
        trellis::intake::markers::discharge_registrations(d.pool())
            .await
            .expect("dispatch the upstream's rebuild");
        write(&d, "insert into public.fa values (10, 4, 20, 1)").await;
        settle_all(&mut d).await;
        assert_eq!(
            statuses(&d).await,
            ["(a1,live)", "(b1,live)", "(j,live)"],
            "{upstream}"
        );
        assert_exact(&mut d, upstream).await;
        step(&mut d, "delete from public.fa where id = 10").await;
        assert_exact(&mut d, upstream).await;
    }
}

/// The `JOIN`'s own `RESUME` over upstream targets: its build waits on the
/// seam fence and rebuilds both sides from the targets.
#[tokio::test]
async fn a_join_resume_over_upstream_targets_rebuilds_it() {
    let mut d = start().await;
    step(&mut d, SEED).await;
    d.ctl
        .batch_execute(
            "update transform_definitions set status = 'paused' where target_table = 'public.j'",
        )
        .await
        .expect("pause the JOIN");
    step(
        &mut d,
        "update public.fa set k = 20 where id = 3; delete from public.db where id = 4",
    )
    .await;
    trellis::staging::quarantine::resume_transform(d.pool(), "j")
        .await
        .expect("resume the JOIN");
    settle_all(&mut d).await;
    assert_eq!(statuses(&d).await, ["(a1,live)", "(b1,live)", "(j,live)"]);
    assert_exact(&mut d, "resumed").await;
    step(&mut d, "insert into public.db values (9, 20, 1, 9)").await;
    assert_exact(&mut d, "written after").await;
}

/// An upstream paused for a capture failure (its source's triggers
/// disabled): writes to its source meanwhile reach neither it nor the
/// `JOIN`, while the other side's keep flowing. Its `RESUME` rebuilds it
/// from its source and rebuilds the `JOIN`, which is exact again.
#[tokio::test]
async fn an_upstream_paused_for_a_capture_failure_is_repaired_by_its_resume() {
    let mut d = start().await;
    step(&mut d, SEED).await;
    d.ctl
        .batch_execute("alter table public.fa disable trigger user")
        .await
        .expect("disable the capture triggers");
    capture_pass(&mut d).await;
    assert_eq!(
        d.rows(
            "select d.status::text, exists (select 1 from capture_failures f \
             where f.transform_id = d.id) from transform_definitions d \
             where d.target_table = 'public.a1'"
        )
        .await,
        ["(paused,t)"],
        "the upstream pauses for the disruption"
    );
    // A row deleted while uncaptured: only the upstream's rebuild removes
    // it from its target, and the `JOIN` must follow that delete.
    step(
        &mut d,
        "update public.fa set k = 20 where id = 3; delete from public.fa where id = 2",
    )
    .await;
    step(&mut d, "insert into public.db values (9, 20, 1, 9)").await;
    assert_eq!(
        d.rows("select status::text from transform_definitions where target_table = 'public.j'")
            .await,
        ["(live)"],
        "the JOIN keeps applying its other side"
    );
    d.ctl
        .batch_execute("alter table public.fa enable trigger user")
        .await
        .expect("enable the capture triggers");
    trellis::staging::quarantine::resume_transform(d.pool(), "a1")
        .await
        .expect("resume the upstream");
    capture_pass(&mut d).await;
    settle_all(&mut d).await;
    assert_eq!(statuses(&d).await, ["(a1,live)", "(b1,live)", "(j,live)"]);
    assert_exact(&mut d, "resumed").await;
    step(&mut d, "update public.fa set k = 10 where id = 3").await;
    assert_exact(&mut d, "written after").await;
}

/// The capture half of one staging-worker pass.
async fn capture_pass(d: &mut Driver) {
    let desired = trellis::defs::tables_to_capture(d.pool())
        .await
        .expect("read the tables to capture");
    trellis::capture::reconcile::reconcile(
        &mut d.ctl,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("capture pass");
}

async fn facade(d: &Driver) -> trellis::Trellis {
    trellis::Trellis::connect(
        trellis::config::Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

/// A `JOIN` reading a paused column of a 1-1 target reports `catching_up`
/// and names the column, as an aggregate does (#1208); one reading the other
/// side's upstream only is unaffected. The column's resume writes the
/// upstream's rows through the seam, so the `JOIN` is exact again with no
/// rebuild.
#[tokio::test]
async fn a_join_reading_a_paused_column_of_a_target_reports_catching_up() {
    let mut d = start().await;
    step(&mut d, SEED).await;
    let trellis = facade(&d).await;
    let reported = |name: &'static str| {
        let trellis = &trellis;
        async move {
            let status = trellis
                .status(name)
                .await
                .expect("status")
                .expect("the definition exists");
            (
                status.status.as_str().to_string(),
                status.paused_upstream_columns,
            )
        }
    };
    trellis
        .apply("PAUSE TRANSFORM a1.w")
        .await
        .expect("pause a1.w");
    assert_eq!(
        reported("j").await,
        ("catching_up".to_string(), vec!["a1.w".to_string()])
    );
    assert_eq!(reported("b1").await, ("live".to_string(), vec![]));
    // The `JOIN` applies over the frozen column, so its sums go stale.
    step(&mut d, "update public.fa set v = 50 where id = 1").await;
    assert_ne!(d.rows(ACTUAL).await, d.rows(ORACLE).await, "stale");
    trellis
        .apply("RESUME TRANSFORM a1.w")
        .await
        .expect("resume a1.w");
    settle_all(&mut d).await;
    assert_eq!(reported("j").await, ("live".to_string(), vec![]));
    assert_exact(&mut d, "the column resumed").await;
}
