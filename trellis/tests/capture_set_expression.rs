//! `ALTER COLUMN … SET EXPRESSION` on a stored generated column (PostgreSQL
//! 17 and later): the statement rewrites every value of the column with no
//! row trigger, and changes the column's `pg_attrdef` expression, so the
//! capture pass's same-type rewrite check (`capture::baseline`, R3b) doesn't
//! act on it. Every reader of the column keeps the old values and stays
//! `live`: the gap entry "A stored generated column's expression replaced by
//! `SET EXPRESSION`" in `docs/known-correctness-gaps.md`. This pins what the
//! entry says: `self_check` reports each kind of reader (a plain 1-1, a 1-1
//! `JOIN` reading either side, an aggregate summing or grouping by the
//! column, a `JOIN` aggregate reading either side), a later write repairs a
//! 1-1 row and not an aggregate group, and `request_backfill` of the table
//! holding the column repairs every reader. On a server older than 17 the
//! test returns without running.
//!
//! Every pass, build and drain is driven by hand; nothing polls or sleeps.

use std::collections::BTreeMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::defs::join_one_to_one::set_enabled;
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

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
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
async fn work(raw: &mut Client, pool: &trellis::Pool, n: usize) {
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

/// Each of `queries`' rows, in turn.
async fn values(raw: &Client, queries: &[&str]) -> Vec<String> {
    let mut values = Vec::new();
    for sql in queries {
        values.extend(rows(raw, sql).await);
    }
    values
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

/// A stored generated column on each side, read by every kind of reader:
/// after `SET EXPRESSION` on `b.g`, every reader of it is stale, `live`, and
/// reported by `self_check`; a write that changes the `b` row repairs the
/// 1-1 rows computed from it and no aggregate group, and a write to the
/// joined `a` row repairs nothing; `request_backfill` of `b` repairs every
/// reader. Then the same for `a.ga` and `request_backfill` of `a`.
#[tokio::test]
async fn set_expression_on_a_stored_generated_column_is_missed_and_repaired_by_a_backfill() {
    set_enabled(true);
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
    let b_readers = vec!["p", "jb", "s", "sg", "j"];
    assert_eq!(
        survey(&raw, &trellis).await,
        (b_readers.clone(), b_readers.clone()),
        "every reader of b.g is stale, live and reported by self_check"
    );

    // A write that changes nothing read stages nothing, so it repairs
    // nothing; nor does one to the `a` row, which `jb` joins to its copy of
    // the `b` row. One that changes `g` (300 to 303) repairs the 1-1 rows
    // computed from the `b` row, and moves an aggregate group by its delta
    // only.
    let of_one = [
        "select g from public.p where bid = 1",
        "select bg from public.jb where id = 1",
        "select total from public.s where grp = 1",
    ];
    for (write, want) in [
        (
            "update public.b set img = img where bid = 1",
            ["(200)", "(200)", "(1800)"],
        ),
        (
            "update public.a set r = r + 1 where id = 1",
            ["(200)", "(200)", "(1800)"],
        ),
        (
            "update public.b set img = img + 1 where bid = 1",
            ["(303)", "(303)", "(1803)"],
        ),
    ] {
        app.batch_execute(write).await.expect(write);
        work(&mut raw, &db.pool, 1).await;
        assert_eq!(values(&raw, &of_one).await, want, "after {write}");
    }

    trellis
        .request_backfill("public.b")
        .await
        .expect("request_backfill b");
    work(&mut raw, &db.pool, 3).await;
    assert_eq!(
        survey(&raw, &trellis).await,
        (vec![], vec![]),
        "request_backfill of b repairs every reader"
    );

    app.batch_execute("alter table public.a alter column ga set expression as (r * 5)")
        .await
        .expect("set a.ga's expression");
    work(&mut raw, &db.pool, 3).await;
    assert_eq!(
        survey(&raw, &trellis).await,
        (vec!["ja", "j"], vec!["ja", "j"]),
        "every reader of a.ga is stale, live and reported by self_check"
    );
    // `ja` reads `b.img` too: a write to the `b` row joins it to `ja`'s copy
    // of the `a` row, and only a write to the `a` row repairs it.
    let of_two = ["select ag, img from public.ja where id = 2"];
    for (write, want) in [
        (
            "update public.b set img = img + 1 where bid = 2",
            ["(40,201)"],
        ),
        ("update public.a set r = r + 1 where id = 2", ["(105,201)"]),
    ] {
        app.batch_execute(write).await.expect(write);
        work(&mut raw, &db.pool, 1).await;
        assert_eq!(values(&raw, &of_two).await, want, "after {write}");
    }
    trellis
        .request_backfill("public.a")
        .await
        .expect("request_backfill a");
    work(&mut raw, &db.pool, 3).await;
    assert_eq!(
        survey(&raw, &trellis).await,
        (vec![], vec![]),
        "request_backfill of a repairs every reader"
    );
    trellis.shutdown().await.expect("shutdown");
}
