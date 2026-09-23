//! Issue #315: every write to a Trellis-owned target table reaches the
//! definitions chained off it through one seam (`staging::target_mutations`),
//! never through CDC. One regression test per writer that used to bypass it,
//! plus the catalog rules that keep the seam the only path: a seam-only
//! target is never published, and nothing can chain off a target that isn't
//! live yet.
//!
//! Drains run by hand (seal, drain, retire until quiescent) rather than
//! through a live client, so each hop lands in its own batch and nothing
//! waits on convergence timing.

use std::collections::{BTreeMap, HashMap};

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{
    CatalogError, Statement, alter_transform, create_definition, create_relationship,
    install_definition, parse_statement, publication_tables,
};
use trellis::staging::quarantine;
use trellis::staging::{
    StagedChange, StagedWatermark, append, apply, has_pending, retire_drained_segments,
};

const WAKE: &str = "seam_wake";

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'"
        ))
        .await
        .expect("set search_path");
    client
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// Seals and drains until nothing is pending, one batch per round.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        trellis::staging::seal_if_active_nonempty(client, WAKE)
            .await
            .expect("seal");
        while let Some(seg) = apply::next_claimable_segment(&*client)
            .await
            .expect("next claimable segment")
        {
            apply::drain_once(pool, seg, "seam_test", 1, WAKE, &watermark)
                .await
                .expect("drain_once");
        }
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

/// Every `recompute` row staged for `src_table` in the active (not yet
/// sealed) segment, as key -> the prior-image hint it carries.
async fn staged_recomputes(raw: &Client, src_table: &str) -> BTreeMap<String, Option<String>> {
    let slot: i16 = raw
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment_pointer")
        .get(0);
    raw.query(
        &format!(
            "select key, old_image::text from seg_{slot} \
             where src_table = $1 and op = 'recompute'"
        ),
        &[&src_table],
    )
    .await
    .expect("read the ring")
    .into_iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

async fn rows(raw: &Client, sql: &str) -> BTreeMap<String, String> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

async fn setup() -> (TestCluster, TestDatabase, Client) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    (cluster, db, raw)
}

/// A seam-only target (some definition's target, no relationship touching
/// it) is never in the publication, even while another definition reads it:
/// its readers hear about it through the seam. A target that is a
/// relationship endpoint stays published — its settled parent projection is
/// driven by CDC.
#[tokio::test]
async fn a_chained_target_is_never_published_unless_it_is_a_relationship_endpoint() {
    let (_cluster, db, raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, k integer, v numeric); \
         alter table public.src replica identity full; \
         create table public.reports (id integer primary key, oid integer); \
         alter table public.reports replica identity full",
    )
    .await
    .expect("create sources");
    let columns = numeric_columns(&["id", "k", "v"]);
    install_definition(
        &db.pool,
        "TRANSFORM public.agg FROM public.src GROUP BY k SELECT COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("install the aggregate");
    // No `REPLICA IDENTITY FULL` on `agg`: an aggregate over a seam-only
    // target never reads its CDC, so it has no old-image requirement.
    install_definition(
        &db.pool,
        "TRANSFORM public.hist FROM public.agg GROUP BY n SELECT COUNT(*) AS groups",
        &numeric_columns(&["k", "n"]),
        "public",
    )
    .await
    .expect("an aggregate chained off an aggregate target needs no replica identity");

    let mut published = publication_tables(&db.pool).await.expect("publication");
    published.sort();
    assert_eq!(
        published,
        vec!["public.src".to_string()],
        "agg is hist's source, but a seam-only target is never published"
    );

    // A relationship endpoint keeps CDC. `t` is a plain 1-1 target (a
    // relationship's join key must be integral, unlike `agg`'s numeric key).
    raw.batch_execute(
        "create table public.t (id integer primary key, doubled numeric); \
         alter table public.t replica identity full",
    )
    .await
    .expect("create t");
    create_definition(
        &db.pool,
        "TRANSFORM public.t FROM public.src SELECT v + v AS doubled",
        &columns,
    )
    .await
    .expect("define t");
    create_relationship(&db.pool, "RELATIONSHIP rollup FROM reports.oid TO t.id")
        .await
        .expect("a relationship whose to-side is a target");
    raw.batch_execute("create table public.report_view (id integer primary key, doubled numeric)")
        .await
        .expect("create report_view");
    create_definition(
        &db.pool,
        "TRANSFORM public.report_view FROM public.reports SELECT rollup.doubled AS doubled",
        &numeric_columns(&["id", "oid"]),
    )
    .await
    .expect("define a reader through the relationship");
    let mut published = publication_tables(&db.pool).await.expect("publication");
    published.sort();
    assert!(
        published.contains(&"public.t".to_string()),
        "a relationship endpoint target stays published: {published:?}"
    );
    assert!(
        !published.contains(&"public.agg".to_string()),
        "{published:?}"
    );
}

/// A target's initial build writes it outside the seam, so a definition may
/// only chain off it once it is live. A plain 1-1 target builds through the
/// chunk queue, and with no drain worker running it stays `backfilling`.
#[tokio::test]
async fn defining_a_transform_off_a_target_that_is_still_backfilling_is_refused() {
    let (_cluster, db, raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, v numeric); \
         insert into public.src values (1, 1), (2, 2)",
    )
    .await
    .expect("create src");
    let h1 = install_definition(
        &db.pool,
        "TRANSFORM public.h1 FROM public.src SELECT v AS v",
        &numeric_columns(&["id", "v"]),
        "public",
    )
    .await
    .expect("install h1");
    assert_eq!(h1.status.as_str(), "backfilling");

    let err = install_definition(
        &db.pool,
        "TRANSFORM public.h2 FROM public.h1 SELECT v + v AS w",
        &numeric_columns(&["id", "v"]),
        "public",
    )
    .await
    .expect_err("h1 isn't live yet");
    match err {
        CatalogError::TransformNotLive { transform, status } => {
            assert_eq!(transform, "h1");
            assert_eq!(status.as_str(), "backfilling");
        }
        other => panic!("expected TransformNotLive, got {other:?}"),
    }
    let h2_exists: bool = raw
        .query_one("select to_regclass('public.h2') is not null", &[])
        .await
        .expect("probe h2")
        .get(0);
    assert!(!h2_exists, "the refusal must come before any DDL");
}

/// `RESUME TRANSFORM agg.col` on an aggregate column used to run the 1-1
/// write-back (`UPDATE <agg> ... WHERE <source primary key> = ...`), which
/// fails outright: an aggregate target has no source-key column.
#[tokio::test]
async fn resuming_an_aggregate_column_succeeds() {
    let (_cluster, db, raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, k integer, v numeric); \
         alter table public.src replica identity full; \
         insert into public.src values (1, 1, 10), (2, 1, 20), (3, 2, 5)",
    )
    .await
    .expect("create src");
    install_definition(
        &db.pool,
        "TRANSFORM public.agg FROM public.src GROUP BY k SELECT SUM(v) AS total",
        &numeric_columns(&["id", "k", "v"]),
        "public",
    )
    .await
    .expect("install agg");
    raw.batch_execute(
        "insert into column_status (transform_table, column_name, last_error, local_fuse) \
         values ('agg', 'total', 'synthetic pause', true)",
    )
    .await
    .expect("pause agg.total");

    let resumed = quarantine::resume_column(&db.pool, "agg", "total")
        .await
        .expect("resuming an aggregate column must not fail");
    assert_eq!(resumed, vec![("agg".to_string(), "total".to_string())]);
    assert_eq!(
        rows(&raw, "select k::text, total::text from public.agg").await,
        BTreeMap::from([
            ("1".to_string(), "30".to_string()),
            ("2".to_string(), "5".to_string()),
        ]),
    );
}

/// Resuming a paused 1-1 column rewrites it on every row it changed, and a
/// definition chained off that target has to hear about each one. The
/// write-back used to be plain `UPDATE`s with no propagation at all.
#[tokio::test]
async fn resuming_a_column_propagates_each_changed_row_to_a_chained_reader() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, v numeric); \
         insert into public.src values (1, 1), (2, 2), (3, 3); \
         create table public.t (id integer primary key, doubled numeric); \
         create table public.d (id integer primary key, quad numeric)",
    )
    .await
    .expect("create tables");
    create_definition(
        &db.pool,
        "TRANSFORM public.t FROM public.src SELECT v + v AS doubled",
        &numeric_columns(&["id", "v"]),
    )
    .await
    .expect("define t");
    create_definition(
        &db.pool,
        "TRANSFORM public.d FROM public.t SELECT doubled + doubled AS quad",
        &numeric_columns(&["id", "doubled"]),
    )
    .await
    .expect("define d");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, "select id::text, quad::text from public.d").await,
        BTreeMap::from([
            ("1".to_string(), "4".to_string()),
            ("2".to_string(), "8".to_string()),
            ("3".to_string(), "12".to_string()),
        ]),
    );

    // Freeze `t.doubled` at a wrong value on two rows (as a tripped column
    // fuse would leave it), then resume it.
    raw.batch_execute(
        "insert into column_status (transform_table, column_name, last_error, local_fuse) \
         values ('t', 'doubled', 'synthetic pause', true); \
         update public.t set doubled = -1 where id in (1, 2)",
    )
    .await
    .expect("freeze t.doubled");
    quarantine::resume_column(&db.pool, "t", "doubled")
        .await
        .expect("resume t.doubled");

    let staged = staged_recomputes(&raw, "public.t").await;
    assert_eq!(
        staged.keys().cloned().collect::<Vec<_>>(),
        vec!["1".to_string(), "2".to_string()],
        "exactly the rows the resume changed must be staged for d"
    );
    assert!(
        staged["1"]
            .as_deref()
            .is_some_and(|img| img.contains("\"-1\"")),
        "each carries its pre-resume image: {staged:?}"
    );
}

/// A source `TRUNCATE` clears every group of an aggregate target. That clear
/// used to be a bare `DELETE FROM <agg>` with no downstream propagation, so a
/// definition chained off the aggregate kept every row it had.
#[tokio::test]
async fn an_aggregate_targets_truncate_clear_reaches_a_chained_reader() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, k text, v numeric); \
         alter table public.src replica identity full; \
         insert into public.src values (1, 'a', 10), (2, 'a', 20), (3, 'b', 5)",
    )
    .await
    .expect("create tables");
    install_definition(
        &db.pool,
        "TRANSFORM public.agg FROM public.src GROUP BY k SELECT SUM(v) AS total",
        &HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("k".to_string(), ValueType::Text),
            ("v".to_string(), ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install agg");
    install_definition(
        &db.pool,
        "TRANSFORM public.d FROM public.agg GROUP BY total SELECT COUNT(*) AS n",
        &HashMap::from([
            ("k".to_string(), ValueType::Text),
            ("total".to_string(), ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install d");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, "select total::text, n::text from public.d").await,
        BTreeMap::from([
            ("30".to_string(), "1".to_string()),
            ("5".to_string(), "1".to_string()),
        ]),
    );

    raw.batch_execute("truncate public.src")
        .await
        .expect("truncate src");
    let txn = raw.transaction().await.expect("begin");
    append(
        &txn,
        &[StagedChange::Truncate {
            src_table: "public.src".to_string(),
            lsn: Some(tokio_postgres::types::PgLsn::from(1)),
            origin_lsn: None,
            src_changed: None,
        }],
    )
    .await
    .expect("stage the truncate");
    txn.commit().await.expect("commit");
    drain_to_quiescence(&db.pool, &mut raw).await;

    assert!(
        rows(&raw, "select k::text, total::text from public.agg")
            .await
            .is_empty()
    );
    assert!(
        rows(&raw, "select total::text, n::text from public.d")
            .await
            .is_empty(),
        "every cleared group must reach d"
    );
}

/// `ALTER TRANSFORM` rewrites a live target's altered column in place; a
/// definition chained off that column has to hear about every changed row.
#[tokio::test]
async fn an_altered_column_propagates_to_a_chained_reader() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, v numeric); \
         insert into public.src values (1, 1), (2, 2); \
         create table public.t (id integer primary key, w numeric); \
         create table public.d (id integer primary key, w numeric)",
    )
    .await
    .expect("create tables");
    create_definition(
        &db.pool,
        "TRANSFORM public.t FROM public.src SELECT v + v AS w",
        &numeric_columns(&["id", "v"]),
    )
    .await
    .expect("define t");
    create_definition(
        &db.pool,
        "TRANSFORM public.d FROM public.t SELECT w AS w",
        &numeric_columns(&["id", "w"]),
    )
    .await
    .expect("define d");
    drain_to_quiescence(&db.pool, &mut raw).await;

    let Statement::AlterTransform(alter) =
        parse_statement("ALTER TRANSFORM t ALTER w AS v + v + v").expect("parse")
    else {
        panic!("not an ALTER");
    };
    alter_transform(&db.pool, &alter).await.expect("alter t.w");
    drain_to_quiescence(&db.pool, &mut raw).await;

    assert_eq!(
        rows(&raw, "select id::text, w::text from public.d").await,
        BTreeMap::from([
            ("1".to_string(), "3".to_string()),
            ("2".to_string(), "6".to_string()),
        ]),
        "d must follow t's altered column"
    );
}
