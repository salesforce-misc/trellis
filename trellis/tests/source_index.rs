//! #1167: a `GROUP BY` (on the group basis) or `JOIN` target needs, on its
//! source, an index its `self_check` walk pages by. Define and a
//! whole-transform `RESUME` refuse without it unless the definition says
//! `WITHOUT SOURCE INDEX`; later `status` warns and `self_check` refuses,
//! and nothing pauses. Every test is stepped by hand: no staging worker runs
//! and nothing polls (#297).

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Config, SelfCheckMode, TransformStatus, Trellis, TrellisOptions};

const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total";
const AGG_OPT_OUT: &str =
    "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total";
const JOIN: &str = "TRANSFORM j FROM public.orders LEFT JOIN public.items \
     ON orders.id = items.order_id GROUP BY orders.region \
     SELECT orders.region AS region, COUNT(*) AS n";

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

/// A database with `src (id, g, h, v)` and a define-only `Trellis`, the
/// group-basis knob on.
async fn setup(extra: &str) -> (TestCluster, testkit::TestDatabase, Client, Trellis) {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(&format!(
        "create table public.src (id integer primary key, g integer, h integer, v integer); \
         create table public.orders (id integer primary key, region text); \
         create table public.items (id integer primary key, order_id integer, qty integer); \
         {extra}"
    ))
    .await
    .expect("create tables");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    (cluster, db, raw, trellis)
}

async fn status_of(trellis: &Trellis, target: &str) -> trellis::DefinitionStatus {
    trellis
        .status(target)
        .await
        .expect("status")
        .expect("defined")
}

async fn refusal(trellis: &Trellis, text: &str) -> String {
    trellis
        .apply(text)
        .await
        .expect_err("define is refused")
        .to_string()
}

#[tokio::test]
async fn define_is_refused_without_the_index_and_names_the_fix() {
    let (_cluster, _db, raw, trellis) = setup("").await;
    let message = refusal(&trellis, AGG).await;
    assert!(message.contains("public.src"), "{message}");
    assert!(message.contains("(g)"), "{message}");
    assert!(
        message.contains(r#"create index on "public"."src" ("g")"#),
        "{message}"
    );
    assert!(message.contains("WITHOUT SOURCE INDEX"), "{message}");
    assert!(trellis.status("agg").await.expect("status").is_none());

    raw.batch_execute("create index on public.src (g)")
        .await
        .expect("index");
    trellis.apply(AGG).await.expect("define with the index");
}

#[tokio::test]
async fn a_key_a_unique_constraint_or_another_column_order_counts() {
    let (_cluster, _db, raw, trellis) = setup(
        "create table public.u (id integer primary key, g integer unique, v integer); \
         create index on public.src (h, g)",
    )
    .await;
    // The primary key leads with the group column.
    trellis
        .apply("TRANSFORM by_id FROM public.src GROUP BY id SELECT SUM(v) AS t")
        .await
        .expect("a primary key counts");
    // A unique constraint leads with it.
    trellis
        .apply("TRANSFORM by_u FROM public.u GROUP BY g SELECT SUM(v) AS t")
        .await
        .expect("a unique constraint counts");
    // The same columns in another order.
    trellis
        .apply("TRANSFORM by_gh FROM public.src GROUP BY g, h SELECT SUM(v) AS t")
        .await
        .expect("the group columns in another order count");
    // Leading columns only: an index with a column in between does not.
    raw.batch_execute("create table public.w (id integer primary key, a int, b int, c int, v int); create index on public.w (a, c, b)")
        .await
        .expect("w");
    let message = refusal(
        &trellis,
        "TRANSFORM by_ab FROM public.w GROUP BY a, b SELECT SUM(v) AS t",
    )
    .await;
    assert!(message.contains("public.w"), "{message}");
}

#[tokio::test]
async fn partial_and_expression_indexes_do_not_count() {
    let (_cluster, _db, raw, trellis) =
        setup("create index on public.src (g) where v > 0; create index on public.src ((g + 1))")
            .await;
    let message = refusal(&trellis, AGG).await;
    assert!(message.contains("create index"), "{message}");
    raw.batch_execute("create index on public.src (g)")
        .await
        .expect("index");
    trellis.apply(AGG).await.expect("a plain index counts");
}

#[tokio::test]
async fn an_index_under_a_pattern_operator_class_does_not_count() {
    // `text_pattern_ops` orders by `~<~`, not `<`: a page's range and
    // `order by` can't use it.
    let (_cluster, _db, raw, trellis) =
        setup("create index on public.orders (region text_pattern_ops)").await;
    let by_region = "TRANSFORM by_region FROM public.orders GROUP BY region SELECT COUNT(*) AS n";
    let message = refusal(&trellis, by_region).await;
    assert!(
        message.contains(r#"create index on "public"."orders" ("region")"#),
        "{message}"
    );
    raw.batch_execute("create index on public.orders (region)")
        .await
        .expect("index");
    trellis
        .apply(by_region)
        .await
        .expect("an index under the default operator class counts");
}

#[tokio::test]
async fn the_opt_out_defines_and_status_reports_it() {
    let (_cluster, _db, raw, trellis) = setup("").await;
    trellis.apply(AGG_OPT_OUT).await.expect("opted out");
    let status = status_of(&trellis, "agg").await;
    assert_eq!(status.source_index_gaps.len(), 1, "{status:?}");
    let gap = &status.source_index_gaps[0];
    assert!(gap.opted_out);
    assert_eq!(
        (gap.table.as_str(), gap.columns.clone()),
        ("public.src", vec!["g".to_string()])
    );
    assert_eq!(gap.fix(), r#"create index on "public"."src" ("g")"#);
    // An opted-out definition's self_check still starts.
    trellis
        .self_check("agg", SelfCheckMode::Standard, Duration::ZERO)
        .await
        .expect("an opted-out definition walks");
    // The index closes the gap.
    raw.batch_execute("create index on public.src (g)")
        .await
        .expect("index");
    assert!(
        status_of(&trellis, "agg")
            .await
            .source_index_gaps
            .is_empty()
    );
}

#[tokio::test]
async fn a_one_to_one_transform_takes_no_opt_out() {
    let (_cluster, _db, _raw, trellis) = setup("").await;
    let message = refusal(
        &trellis,
        "TRANSFORM one FROM public.src WITHOUT SOURCE INDEX SELECT v AS v",
    )
    .await;
    assert!(message.contains("WITHOUT SOURCE INDEX"), "{message}");
}

#[tokio::test]
async fn a_dropped_index_warns_refuses_self_check_and_pauses_nothing() {
    let (_cluster, _db, raw, trellis) = setup("create index on public.src (g)").await;
    trellis.apply(AGG).await.expect("define");
    assert!(
        status_of(&trellis, "agg")
            .await
            .source_index_gaps
            .is_empty()
    );
    let before = status_of(&trellis, "agg").await.status;

    raw.batch_execute("drop index public.src_g_idx")
        .await
        .expect("drop");
    let status = status_of(&trellis, "agg").await;
    assert_eq!(status.status, before, "nothing pauses");
    assert_ne!(status.status, TransformStatus::Paused);
    assert_eq!(status.source_index_gaps.len(), 1);
    assert!(!status.source_index_gaps[0].opted_out);

    let err = trellis
        .self_check("agg", SelfCheckMode::Standard, Duration::ZERO)
        .await
        .expect_err("self_check refuses");
    let message = err.to_string();
    assert!(
        message.contains(r#"create index on "public"."src" ("g")"#),
        "{message}"
    );
}

#[tokio::test]
async fn resume_refuses_without_the_index_and_succeeds_after_create_index() {
    let (_cluster, _db, raw, trellis) = setup("create index on public.items (order_id)").await;
    trellis.apply(JOIN).await.expect("define");
    trellis.apply("PAUSE TRANSFORM j").await.expect("pause");
    raw.batch_execute("drop index public.items_order_id_idx")
        .await
        .expect("drop");
    let message = refusal(&trellis, "RESUME TRANSFORM j").await;
    assert!(
        message.contains(r#"create index on "public"."items" ("order_id")"#),
        "{message}"
    );
    assert_eq!(
        status_of(&trellis, "j").await.status,
        TransformStatus::Paused
    );
    raw.batch_execute("create index on public.items (order_id)")
        .await
        .expect("index");
    trellis.apply("RESUME TRANSFORM j").await.expect("resume");
}

#[tokio::test]
async fn a_join_checks_each_side_separately() {
    let (_cluster, _db, raw, trellis) = setup("").await;
    // orders.id is its primary key; items.order_id has no index.
    let message = refusal(&trellis, JOIN).await;
    assert!(message.contains("public.items"), "{message}");
    assert!(!message.contains("public.orders has none"), "{message}");
    assert!(
        message.contains(r#"create index on "public"."items" ("order_id")"#),
        "{message}"
    );

    raw.batch_execute("create index on public.items (order_id)")
        .await
        .expect("items index");
    trellis.apply(JOIN).await.expect("both sides indexed");

    // Dropping the joined side's index reports only that side.
    raw.batch_execute("drop index public.items_order_id_idx")
        .await
        .expect("drop");
    let gaps = status_of(&trellis, "j").await.source_index_gaps;
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    assert_eq!(gaps[0].table, "public.items");
}
