//! `ALTER COLUMN … SET EXPRESSION` on a stored generated column (PostgreSQL
//! 17 and later): the statement rewrites every value of the column with no
//! row trigger. The capture pass sees the column's expression changed as the
//! table was rewritten (`capture::baseline`) and rebuilds every applying
//! reader of the table, pausing none, under the `generated_expression` cause.
//! This covers each kind of reader: a plain 1-1, a 1-1 `JOIN` reading either
//! side, an aggregate summing or grouping by the column, and a `JOIN`
//! aggregate reading either side. On a server older than 17 the test returns
//! without running.
//!
//! Every pass, build and drain is driven by hand; nothing polls or sleeps.

use std::collections::BTreeMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::staging::self_check::CheckPageExt;
use trellis::{Config, SelfCheckMode, SelfCheckOutcome, Trellis, TrellisOptions};

const SCHEMA: &str = "trellis";
const WAKE: &str = "capture_set_expression_wake";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

async fn drain_to_quiescence(pool: &trellis::pool::Pool, client: &mut Client) {
    use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, WAKE)
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_set_expression_test",
            1,
            WAKE,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("the ring did not reach quiescence within 16 seal/drain rounds");
}

/// `n` rounds of a whole staging-worker pass (capture, then the start of
/// ready builds), the builds run, and the ring drained.
async fn work(raw: &mut Client, pool: &trellis::pool::Pool, n: usize) {
    for _ in 0..n {
        trellis::client::reconcile_pass(raw, pool, SCHEMA)
            .await
            .expect("reconcile pass");
        trellis::staging::build::settle_builds(pool).await;
        drain_to_quiescence(pool, raw).await;
    }
}

/// Every definition's status, by bare target name.
async fn statuses(raw: &Client) -> BTreeMap<String, String> {
    raw.query(
        "select split_part(target_table, '.', 2), status::text from transform_definitions",
        &[],
    )
    .await
    .expect("read statuses")
    .into_iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

async fn rows(raw: &Client, sql: &str) -> Vec<String> {
    raw.query(&format!("select x::text from ({sql}) x"), &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

/// Each reader: its target, a query over it, and the same query over the
/// live sources, both ordered.
const READERS: [(&str, &str, &str); 6] = [
    (
        "p",
        "select bid, g from public.p order by bid",
        "select bid, g from public.b order by bid",
    ),
    (
        "jb",
        "select id, r, bg from public.jb order by id",
        "select a.id, a.r, b.g from public.a a left join public.b b on a.k = b.k order by a.id",
    ),
    (
        "ja",
        "select id, ag, img from public.ja order by id",
        "select a.id, a.ga, b.img from public.a a left join public.b b on a.k = b.k \
         order by a.id",
    ),
    (
        "s",
        "select grp, total from public.s order by grp",
        "select grp, sum(g) from public.b group by grp order by grp",
    ),
    (
        "sg",
        "select g, c from public.sg order by g",
        "select g, count(*) from public.b group by g order by g",
    ),
    (
        "j",
        "select grp, bt, at from public.j order by grp",
        "select a.grp, sum(b.g), sum(a.ga) from public.a a join public.b b on a.k = b.k \
         group by a.grp order by a.grp",
    ),
];

/// Which readers' targets equal their live sources, and which `self_check`
/// reports diverged, each as a sorted list of target names; asserts every
/// reader is `live` and every `self_check` gives a verdict.
async fn survey(raw: &Client, trellis: &Trellis) -> (Vec<&'static str>, Vec<&'static str>) {
    let statuses = statuses(raw).await;
    assert!(
        statuses.values().all(|s| s == "live"),
        "every reader stays live: {statuses:?}"
    );
    let mut stale = Vec::new();
    let mut diverged = Vec::new();
    for (target, of_target, of_source) in READERS {
        if rows(raw, of_target).await != rows(raw, of_source).await {
            stale.push(target);
        }
        let report = trellis
            .self_check_to_end(target, SelfCheckMode::Strict, Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("self_check {target}: {e}"));
        match report.outcome {
            SelfCheckOutcome::Converged => {}
            SelfCheckOutcome::Diverged(_) => diverged.push(target),
            other => panic!("self_check {target} gave no verdict: {other:?}"),
        }
    }
    (stale, diverged)
}

/// How many times the pass rebuilt `table`'s readers for `cause`.
fn auto_rebuilds(table: &str, cause: &str) -> u64 {
    trellis::Metrics::new()
        .render_prometheus()
        .lines()
        .filter(|line| {
            line.starts_with("trellis_capture_auto_rebuilds_total{")
                && line.contains(&format!("source_table=\"{table}\""))
                && line.contains(&format!("cause=\"{cause}\""))
        })
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|n| n.parse::<f64>().ok())
                .expect("a counter value") as u64
        })
        .sum()
}

/// A stored generated column on each side, read by every kind of reader:
/// after `SET EXPRESSION` on `b.g`, the next passes rebuild every reader of
/// `b`, which ends equal to its sources and `live`; then the same for `a.ga`.
#[tokio::test]
async fn set_expression_on_a_stored_generated_column_rebuilds_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let version: i32 = raw
        .query_one("select current_setting('server_version_num')::int", &[])
        .await
        .expect("read the server version")
        .get(0);
    if version < 170_000 {
        eprintln!("skipped: SET EXPRESSION needs PostgreSQL 17, the server is {version}");
        return;
    }
    raw.batch_execute(
        "create table public.b (bid int primary key, k int not null unique, grp int not null, \
             img int, g int generated always as (img * 2) stored); \
         create table public.a (id int primary key, k int, grp int not null, r int, \
             ga int generated always as (r * 2) stored); \
         insert into public.b (bid, k, grp, img) \
           select i, i, i % 2, i * 100 from generate_series(1, 6) i; \
         insert into public.a (id, k, grp, r) \
           select i, i, i % 3, i * 10 from generate_series(1, 6) i;",
    )
    .await
    .expect("create the sources");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer");
    for text in [
        "TRANSFORM p FROM public.b SELECT g AS g",
        "TRANSFORM jb FROM public.a LEFT JOIN public.b ON a.k = b.k \
         SELECT a.r AS r, b.g AS bg",
        "TRANSFORM ja FROM public.a LEFT JOIN public.b ON a.k = b.k \
         SELECT a.ga AS ag, b.img AS img",
        "TRANSFORM s FROM public.b GROUP BY grp WITHOUT SOURCE INDEX \
         SELECT grp AS grp, SUM(g) AS total",
        "TRANSFORM sg FROM public.b GROUP BY g WITHOUT SOURCE INDEX \
         SELECT g AS g, COUNT(*) AS c",
        "TRANSFORM j FROM public.a JOIN public.b ON a.k = b.k GROUP BY a.grp \
         WITHOUT SOURCE INDEX SELECT a.grp AS grp, SUM(b.g) AS bt, SUM(a.ga) AS at",
    ] {
        trellis.apply(text).await.expect(text);
    }
    work(&mut raw, &db.pool, 3).await;
    assert_eq!(
        survey(&raw, &trellis).await,
        (vec![], vec![]),
        "every reader equals its sources before the ALTER"
    );

    let app = connect(db.dsn()).await;
    app.batch_execute("alter table public.b alter column g set expression as (img * 3)")
        .await
        .expect("set b.g's expression");
    work(&mut raw, &db.pool, 3).await;
    assert_eq!(
        rows(&raw, "select g from public.p order by bid").await,
        ["(300)", "(600)", "(900)", "(1200)", "(1500)", "(1800)"],
        "p holds the values of the new expression"
    );
    assert_eq!(
        survey(&raw, &trellis).await,
        (vec![], vec![]),
        "every reader of b.g is rebuilt and live"
    );
    assert_eq!(auto_rebuilds("public.b", "generated_expression"), 1);

    app.batch_execute("alter table public.a alter column ga set expression as (r * 5)")
        .await
        .expect("set a.ga's expression");
    work(&mut raw, &db.pool, 3).await;
    assert_eq!(
        survey(&raw, &trellis).await,
        (vec![], vec![]),
        "every reader of a.ga is rebuilt and live"
    );
    assert_eq!(auto_rebuilds("public.a", "generated_expression"), 1);
    assert_eq!(auto_rebuilds("public.b", "value_rewrite"), 0);
    assert_eq!(auto_rebuilds("public.a", "value_rewrite"), 0);
    trellis.shutdown().await.expect("shutdown");
}
