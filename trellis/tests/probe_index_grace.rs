//! The capture pass's check of a `MIN`/`MAX` definition's probe index
//! (#1708 rule 2): an index leading with the `GROUP BY` columns then the
//! argument, read missing or invalid on every check for the grace (5 minutes,
//! a constant), pauses the definition, naming the index to create. A check
//! that finds it resets the clock, and an equivalent index pauses nothing.
//!
//! The grace is set per instance through the test-only override
//! (`trellis::staging::set_extreme_index_grace`): zero to pause on the first
//! miss, an hour to watch the clock run. Every pass and drain is stepped by
//! hand. Nothing sleeps or polls.

#[path = "support/resume_passes.rs"]
mod resume_passes;

use std::time::Duration;

use resume_passes::{SCHEMA, bring_live, drain_to_quiescence, full_pass, rows, setup, status};
use testkit::TestCluster;
use trellis::defs::TransformStatus;

const SOURCE: &str = "create table public.src (id int primary key, g int not null, v int); \
     create index src_g_idx on public.src (g); \
     create index src_g_v_idx on public.src (g, v); \
     insert into public.src select i, i % 3, i * 7 % 11 from generate_series(1, 12) i";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g \
     SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n";

async fn converged(raw: &tokio_postgres::Client) {
    assert_eq!(
        rows(
            raw,
            "select g::text, lo::text, hi::text, n::text from public.agg order by g"
        )
        .await,
        rows(
            raw,
            "select g::text, min(v)::text, max(v)::text, count(*)::text from public.src \
             group by g order by g"
        )
        .await
    );
}

async fn database(raw: &tokio_postgres::Client) -> String {
    raw.query_one("select current_database()::text", &[])
        .await
        .unwrap()
        .get(0)
}

/// `setup`, with the probe grace of the new database's instance set to
/// `grace` before any pass checks it.
async fn start(
    cluster: &TestCluster,
    grace: Duration,
) -> (
    testkit::TestDatabase,
    tokio_postgres::Client,
    trellis::Trellis,
) {
    let (db, raw, trellis) = setup(cluster, SOURCE, &[AGG]).await;
    trellis::staging::set_extreme_index_grace(&database(&raw).await, SCHEMA, grace);
    (db, raw, trellis)
}

const PAUSED: &str = "no valid index on public.src has led with (g, v) in that order for 5 \
     minutes, which this definition's MIN/MAX(v) re-reads a group's extreme through: without one \
     each re-read scans the group. Create one with `create index on \"public\".\"src\" (\"g\", \
     \"v\")`, then resume the definition to rebuild it. To replace the index, build the \
     replacement before dropping the old one";

/// With the grace passed (zero here), the next pass after the probe index is
/// dropped pauses the definition with the index to create. `RESUME` refuses
/// while it is missing; once it is back the message says the cause is gone,
/// and `RESUME` rebuilds.
#[tokio::test]
async fn the_probe_index_pauses_once_its_grace_has_passed() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = start(&cluster, Duration::ZERO).await;
    raw.batch_execute("drop index public.src_g_v_idx")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    let reported = status(&trellis, "agg").await;
    assert_eq!(reported.status, TransformStatus::Paused);
    let failure = reported.capture_failure.expect("why");
    assert_eq!(failure.source_table, "public.src");
    assert_eq!(failure.columns, ["g", "v"]);
    assert_eq!(failure.reasons, [PAUSED]);
    let refusal = trellis
        .apply("RESUME TRANSFORM agg")
        .await
        .expect_err("the probe index is missing")
        .to_string();
    assert!(
        refusal.contains("needs a source index for its MIN/MAX"),
        "{refusal}"
    );

    raw.batch_execute("create index src_g_v_idx on public.src (g, v)")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    let failure = status(&trellis, "agg")
        .await
        .capture_failure
        .expect("still paused");
    assert_eq!(
        failure.error,
        "the cause it was paused for is gone. RESUME would rebuild public.agg from public.src"
    );
    trellis.apply("RESUME TRANSFORM agg").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["agg"]).await;
    converged(&raw).await;
}

/// Inside the grace the definition keeps running exact, and the pass checks
/// the index on every pass while the clock runs. The index back resets the
/// clock, and from then on an unchanged table costs no check.
#[tokio::test]
async fn inside_the_grace_nothing_pauses_and_a_found_index_stops_the_clock() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = start(&cluster, Duration::from_secs(3600)).await;
    let database = database(&raw).await;
    let id: i64 = raw
        .query_one(
            "select id from trellis.transform_definitions where target_table = 'public.agg'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let checks = || trellis::staging::probe_index_checks(&database, SCHEMA, id);
    full_pass(&mut raw, &db.pool).await;
    let settled = checks();
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(checks(), settled, "an unchanged table is not checked again");

    raw.batch_execute(
        "drop index public.src_g_v_idx; delete from public.src where id = 3; \
         update public.src set v = 0 where id = 4",
    )
    .await
    .unwrap();
    for pass in 1..=3 {
        full_pass(&mut raw, &db.pool).await;
        drain_to_quiescence(&db.pool, &mut raw).await;
        let reported = status(&trellis, "agg").await;
        assert_eq!(reported.status, TransformStatus::Live, "pass {pass}");
        assert_eq!(reported.capture_failure, None, "pass {pass}");
        assert_eq!(reported.extreme_index_gaps.len(), 1, "pass {pass}");
        assert_eq!(checks(), settled + pass, "checked while the clock runs");
    }
    converged(&raw).await;

    raw.batch_execute("create index src_g_v_idx on public.src (g, v)")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    let found = checks();
    assert_eq!(found, settled + 4);
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(
        checks(),
        found,
        "the clock stopped and the table is unchanged"
    );
    assert_eq!(status(&trellis, "agg").await.status, TransformStatus::Live);
}

/// An equivalent index pauses nothing even with no grace: one built before
/// the old one is dropped, a drop and create in one transaction, and a
/// `REINDEX CONCURRENTLY`.
#[tokio::test]
async fn an_equivalent_index_pauses_nothing_even_with_no_grace() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = start(&cluster, Duration::ZERO).await;
    for statement in [
        "create index concurrently src_g_v_new on public.src (g, v)",
        "drop index concurrently public.src_g_v_idx",
        "alter index public.src_g_v_new rename to src_g_v_idx",
        "begin; drop index public.src_g_v_idx; create index src_g_v_idx on public.src (g, v); \
         commit",
        "reindex index concurrently public.src_g_v_idx",
        // A failed build of another index leaves an invalid one beside it.
        "create unique index concurrently src_bad on public.src (g)",
    ] {
        let outcome = raw.batch_execute(statement).await;
        if statement.contains("src_bad") {
            outcome.expect_err("g has duplicates");
        } else {
            outcome.unwrap_or_else(|e| panic!("{statement}: {e}"));
        }
        full_pass(&mut raw, &db.pool).await;
        let reported = status(&trellis, "agg").await;
        assert_eq!(reported.status, TransformStatus::Live, "{statement}");
        assert_eq!(reported.capture_failure, None, "{statement}");
    }
    raw.batch_execute("delete from public.src where id = 5")
        .await
        .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    converged(&raw).await;
}
