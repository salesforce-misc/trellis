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
use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{
    CatalogError, Statement, alter_transform, create_definition, create_relationship,
    install_definition, parse_statement, publication_tables,
};
use trellis::intake::publication;
use trellis::integer::IntWidth;
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

/// A target is never in the publication, even while another definition
/// reads it: its readers hear about it through the seam. Since #403 that
/// includes a target that is a relationship endpoint, whose settled parent
/// projection the seam's CDC-shaped rows drive.
#[tokio::test]
async fn a_chained_target_is_never_published_even_as_a_relationship_endpoint() {
    let (_cluster, db, raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, k integer, v numeric); \
         alter table public.src replica identity full; \
         create table public.reports (id integer primary key, oid integer); \
         alter table public.reports replica identity full",
    )
    .await
    .expect("create sources");
    let mut columns = numeric_columns(&["id", "k", "v"]);
    // `k` is `agg`'s identity, which `hist` keys on: it must be a text-stable
    // key type, so not `numeric` (issue #371).
    columns.insert("k".to_string(), ValueType::Integer(IntWidth::Int4));
    install_definition(
        &db.pool,
        "TRANSFORM public.agg FROM public.src GROUP BY k SELECT COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("install the aggregate");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    // No `REPLICA IDENTITY FULL` on `agg`: an aggregate over a seam-only
    // target never reads its CDC, so it has no old-image requirement.
    install_definition(
        &db.pool,
        "TRANSFORM public.hist FROM public.agg GROUP BY n SELECT COUNT(*) AS groups",
        &HashMap::from([
            ("k".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("n".to_string(), ValueType::Numeric),
        ]),
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

    // A relationship endpoint is no exception. `t` is a plain 1-1 target, on
    // its default replica identity.
    raw.batch_execute("create table public.t (id integer primary key, doubled numeric)")
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
    assert_eq!(
        published,
        vec!["public.reports".to_string(), "public.src".to_string()],
        "only true sources are published, not the endpoint target t"
    );
}

/// A target's initial build writes it outside the seam, so a definition may
/// only chain off it once it is live. A plain 1-1 target builds through the
/// chunk queue its discharge dispatches, and with no drain worker running it
/// stays `backfilling`.
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
    assert_eq!(h1.status.as_str(), "waiting_to_backfill");
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("dispatch h1's chunks");

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
    trellis::intake::publication::settle_registrations(&db.pool).await;
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
    trellis::intake::publication::settle_registrations(&db.pool).await;
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
    trellis::intake::publication::settle_registrations(&db.pool).await;
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

/// Issue #385: the same truncate clear over an aggregate grouped by a
/// `numeric` column. `numeric` is an admitted `GROUP BY` type (only the
/// *source's* key type is gated, issue #371), so the aggregate installs, but
/// the clear used to look up its target's identity through the key-type-gated
/// `ddl::source_primary_key`, got `UnsupportedPrimaryKeyType`, and halted the
/// instance. The clear only needs the identity's column names to report each
/// cleared group; it must not care about their type.
#[tokio::test]
async fn a_numeric_grouped_aggregates_truncate_clear_drains_without_halting() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, p numeric, q integer); \
         alter table public.src replica identity full; \
         insert into public.src values (1, 1.5, 10), (2, 1.5, 20), (3, 2, 5)",
    )
    .await
    .expect("create tables");
    install_definition(
        &db.pool,
        "TRANSFORM public.agg FROM public.src GROUP BY p SELECT SUM(q) AS t",
        &numeric_columns(&["id", "p", "q"]),
        "public",
    )
    .await
    .expect("a numeric-grouped aggregate over an integer-keyed table is accepted");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, "select p::text, t::text from public.agg").await,
        BTreeMap::from([
            ("1.5".to_string(), "30".to_string()),
            ("2".to_string(), "5".to_string()),
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
        rows(&raw, "select p::text, t::text from public.agg")
            .await
            .is_empty(),
        "the truncate must clear every group"
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

/// A to-side change re-derives a relationship-reading aggregate through the
/// reverse-relationship fast path, which writes the aggregate target itself.
/// Those writes have to reach a transform chained off that target like any
/// other: before the seam, the fast path's bookkeeping was keyed differently
/// from the reader lookup, so its writes were never propagated in the
/// applying transaction.
#[tokio::test]
async fn a_reverse_relationship_write_to_an_aggregate_target_reaches_a_chained_reader() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.posts (id integer primary key, word_count integer); \
         create table public.post_tags (id integer primary key, post integer, tag text); \
         alter table public.post_tags replica identity full; \
         alter table public.posts replica identity full; \
         create index on public.post_tags (post); \
         insert into public.posts values (1, 100), (2, 250); \
         insert into public.post_tags values (10, 1, 'rust'), (11, 2, 'rust'), (12, 1, 'db')",
    )
    .await
    .expect("create tables");
    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create the relationship");
    install_definition(
        &db.pool,
        "TRANSFORM public.tag_totals FROM public.post_tags GROUP BY tag \
         SELECT SUM(post.word_count) AS total_words",
        &HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("post".to_string(), ValueType::Numeric),
            ("tag".to_string(), ValueType::Text),
        ]),
        "public",
    )
    .await
    .expect("install tag_totals");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    install_definition(
        &db.pool,
        "TRANSFORM public.tag_view FROM public.tag_totals GROUP BY tag \
         SELECT SUM(total_words) AS total_words",
        &HashMap::from([
            ("tag".to_string(), ValueType::Text),
            ("total_words".to_string(), ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install tag_view");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    raw.execute("update public.posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    let txn = raw.transaction().await.expect("begin");
    append(
        &txn,
        &[StagedChange::Cdc {
            src_table: "public.posts".to_string(),
            key: "1".to_string(),
            op: trellis::staging::CdcOp::Update,
            lsn: Some(tokio_postgres::types::PgLsn::from(1)),
            old_image: Some(r#"{"id":"1","word_count":"100"}"#.to_string()),
            new_image: Some(r#"{"id":"1","word_count":"400"}"#.to_string()),
            origin_lsn: None,
            src_changed: None,
            hop_gen: 0,
            group_key: None,
        }],
    )
    .await
    .expect("stage the to-side update");
    txn.commit().await.expect("commit");
    drain_to_quiescence(&db.pool, &mut raw).await;

    let expected = BTreeMap::from([
        ("db".to_string(), "400".to_string()),
        ("rust".to_string(), "650".to_string()),
    ]);
    assert_eq!(
        rows(&raw, "select tag, total_words::text from public.tag_totals").await,
        expected,
    );
    assert_eq!(
        rows(&raw, "select tag, total_words::text from public.tag_view").await,
        expected,
        "tag_view must follow the reverse fast path's write to tag_totals"
    );
}

/// A definition created through the ring path enumerates its source inside
/// its creating transaction. When that source is another definition's
/// target, a write to it that commits after the enumeration's read, by a
/// writer whose seam checked for readers before the new definition
/// existed, stages nothing, and the target has no CDC to fall back on. The
/// catch-up parked after the creating transaction commits is what catches
/// it. The racing write is a raw insert standing in for that writer.
#[tokio::test]
async fn a_target_write_racing_a_chained_definitions_creation_is_caught_up() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, v numeric); \
         insert into public.src values (1, 1); \
         create table public.t (id integer primary key, doubled numeric); \
         create table public.d (id integer primary key, doubled numeric)",
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
    drain_to_quiescence(&db.pool, &mut raw).await;

    let mut racer = connect_raw(db.dsn()).await;
    let racing = racer.transaction().await.expect("begin the racing write");
    racing
        .execute("insert into public.t values (99, 42)", &[])
        .await
        .expect("racing insert");
    create_definition(
        &db.pool,
        "TRANSFORM public.d FROM public.t SELECT doubled AS doubled",
        &numeric_columns(&["id", "doubled"]),
    )
    .await
    .expect("define d while the write is in flight");
    racing.commit().await.expect("commit the racing write");

    drain_to_quiescence(&db.pool, &mut raw).await;
    publication::run_pending_backfills(
        &mut raw,
        WAKE,
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("run pending backfills");
    drain_to_quiescence(&db.pool, &mut raw).await;

    assert_eq!(
        rows(&raw, "select id::text, doubled::text from public.d").await,
        BTreeMap::from([
            ("1".to_string(), "2".to_string()),
            ("99".to_string(), "42".to_string()),
        ]),
        "the write that raced d's creation must still reach d"
    );
}

/// `recompute_column` writes back in bounded transactions, one per chunk of
/// keys, so a resume never holds row locks (and an xid) across the whole
/// target. Every changed row in every chunk still reaches a chained reader.
#[tokio::test]
async fn resuming_a_column_across_several_chunks_propagates_every_changed_row() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, v numeric); \
         insert into public.src select g, g from generate_series(1, 2500) g; \
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

    raw.batch_execute(
        "insert into column_status (transform_table, column_name, last_error, local_fuse) \
         values ('t', 'doubled', 'synthetic pause', true); \
         update public.t set doubled = -1 where id % 2 = 0",
    )
    .await
    .expect("freeze t.doubled");
    quarantine::resume_column(&db.pool, "t", "doubled")
        .await
        .expect("resume t.doubled");

    let staged = staged_recomputes(&raw, "public.t").await;
    assert_eq!(staged.len(), 1250, "exactly the rows the resume changed");
    assert!(
        staged
            .values()
            .all(|img| img.as_deref().is_some_and(|img| img.contains("\"-1\""))),
        "each carries its pre-resume image"
    );
    let wrong: i64 = raw
        .query_one(
            "select count(*) from public.t join public.src using (id) where doubled <> v + v",
            &[],
        )
        .await
        .expect("count wrong rows")
        .get(0);
    assert_eq!(wrong, 0, "every chunk wrote its rows back");

    drain_to_quiescence(&db.pool, &mut raw).await;
    let wrong: i64 = raw
        .query_one(
            "select count(*) from public.d join public.src using (id) where quad <> 4 * v",
            &[],
        )
        .await
        .expect("count wrong downstream rows")
        .get(0);
    assert_eq!(wrong, 0, "d follows every resumed row");
}

/// A resumed definition's rebuild writes its target outside the seam, and
/// the target's readers have to hear of what it changed. A relationship
/// declared on the target while it was `live` survives a pause, and a
/// consumer reading through it doesn't read the target's table as its
/// source, so the rebuild's go-live catch-up for readers
/// (`park_target_catchup_if_read`) doesn't cover it. A source change made
/// during the pause reaches the target through the rebuild, but never the
/// consumer.
#[tokio::test]
#[ignore = "#507: a resumed target's rebuild never reaches a relationship consumer"]
async fn a_resumed_targets_rebuild_reaches_a_relationship_consumer() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.orders (id integer primary key, a numeric); \
         alter table public.orders replica identity full; \
         insert into public.orders values (1, 1), (2, 2); \
         create table public.order_doubles (id integer primary key, x numeric); \
         create table public.reports (id integer primary key, oid integer); \
         alter table public.reports replica identity full; \
         insert into public.reports values (1, 1); \
         create table public.report_view (id integer primary key, x numeric)",
    )
    .await
    .expect("create tables");
    create_definition(
        &db.pool,
        "TRANSFORM public.order_doubles FROM public.orders SELECT a + a AS x",
        &numeric_columns(&["id", "a"]),
    )
    .await
    .expect("define order_doubles");
    drain_to_quiescence(&db.pool, &mut raw).await;
    publication::settle_registrations(&db.pool).await;
    create_relationship(
        &db.pool,
        "RELATIONSHIP rollup FROM reports.oid TO order_doubles.id",
    )
    .await
    .expect("a relationship on the live target");
    create_definition(
        &db.pool,
        "TRANSFORM public.report_view FROM public.reports SELECT rollup.x AS x",
        &numeric_columns(&["id", "oid"]),
    )
    .await
    .expect("define a consumer through the relationship");
    drain_to_quiescence(&db.pool, &mut raw).await;
    publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, "select id::text, x::text from public.report_view").await,
        BTreeMap::from([("1".to_string(), "2".to_string())]),
        "precondition: the consumer reads the target through the relationship"
    );

    let trellis = trellis::Trellis::connect(
        trellis::Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis");
    trellis
        .apply("PAUSE TRANSFORM order_doubles")
        .await
        .expect("pause the target");

    // A source change while the target is paused, staged as intake would. Its
    // apply skips the frozen target, and the rebuild picks it up.
    raw.execute("update public.orders set a = 100 where id = 1", &[])
        .await
        .expect("update the source");
    let txn = raw.transaction().await.expect("begin");
    append(
        &txn,
        &[StagedChange::Cdc {
            src_table: "public.orders".to_string(),
            key: "1".to_string(),
            op: trellis::staging::CdcOp::Update,
            lsn: Some(tokio_postgres::types::PgLsn::from(1)),
            old_image: Some(r#"{"id":"1","a":"1"}"#.to_string()),
            new_image: Some(r#"{"id":"1","a":"100"}"#.to_string()),
            origin_lsn: None,
            src_changed: None,
            hop_gen: 0,
            group_key: None,
        }],
    )
    .await
    .expect("stage the source update");
    txn.commit().await.expect("commit");
    drain_to_quiescence(&db.pool, &mut raw).await;

    trellis
        .apply("RESUME TRANSFORM order_doubles")
        .await
        .expect("resume the target");
    publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    let status: String = raw
        .query_one(
            "select status from transform_definitions where target_table = 'public.order_doubles'",
            &[],
        )
        .await
        .expect("read status")
        .get(0);
    assert_eq!(status, "live", "precondition: the rebuild went live");
    assert_eq!(
        rows(
            &raw,
            "select id::text, x::text from public.order_doubles order by id"
        )
        .await,
        BTreeMap::from([
            ("1".to_string(), "200".to_string()),
            ("2".to_string(), "4".to_string()),
        ]),
        "precondition: the rebuild read the change"
    );
    assert_eq!(
        rows(&raw, "select id::text, x::text from public.report_view").await,
        BTreeMap::from([("1".to_string(), "200".to_string())]),
        "the rebuild's change to the target must reach the relationship consumer"
    );
}
