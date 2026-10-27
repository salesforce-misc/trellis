//! A source whose primary key changed while its readers were paused:
//! dropped and created again with another key. The keys Trellis recorded or
//! staged under the old key are discarded by the resume, never decoded
//! against the new one, and the rebuild reads the table now at the name.
//!
//! The aggregate and the 1-1 target; a `JOIN` is in
//! `group_basis_resume_after_source_key_change.rs`. Every pass, build and
//! drain is stepped by hand.
//! Nothing polls for convergence.

#[path = "support/resume_passes.rs"]
mod resume_passes;

use resume_passes::{
    RECREATED, SOURCE, assert_staged, bring_live, capture_pass, drain_to_quiescence, rows, setup,
    status,
};
use testkit::TestCluster;
use trellis::defs::TransformStatus;

const SUMS: &str =
    "TRANSFORM c_sums FROM public.c GROUP BY pid WITHOUT SOURCE INDEX SELECT SUM(amount) AS total";
const SUMS_ACTUAL: &str = "select pid::text, total::text from public.c_sums order by 1";
const SUMS_ORACLE: &str =
    "select pid::text, sum(amount)::text from public.c group by pid order by 1";

/// An aggregate's source dropped and created again with a
/// two-column key, with a change staged under the old key and not yet
/// drained. The resume rebuilds the target from the new table, and the new
/// table's changes apply after it.
#[tokio::test]
async fn an_aggregate_resumes_onto_a_source_recreated_with_a_wider_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    // Staged under the old key, and left in the ring.
    raw.batch_execute("insert into public.c values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    raw.batch_execute("insert into public.c values (4, 8, 1); delete from public.c where id = 1")
        .await
        .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// The reverse: a two-column key narrowed to one. A key staged under the
/// old key splits into no part of the new one, and would cast as the whole
/// key text, so nothing may decode it.
#[tokio::test]
async fn an_aggregate_resumes_onto_a_source_recreated_with_a_narrower_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.c (id int not null, pid int not null, amount int, \
             primary key (pid, id)); \
         create index on public.c (pid); \
         insert into public.c select i, i % 3, i from generate_series(1, 6) i",
        &[SUMS],
    )
    .await;
    raw.batch_execute("insert into public.c values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(
        "drop table public.c; \
         create table public.c (id int primary key, pid int not null, amount int); \
         create index on public.c (pid); \
         insert into public.c values (1, 7, 40), (2, 7, 60), (3, 8, 1000)",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// The same columns in the other order: every old key still splits into as
/// many parts as the new key has, and its parts would bind to the wrong
/// columns (`'a'` as an `integer`).
#[tokio::test]
async fn an_aggregate_resumes_onto_a_source_whose_key_columns_were_reordered() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.c (id int not null, tag text not null, pid int not null, \
             amount int, primary key (id, tag)); \
         create index on public.c (pid); \
         insert into public.c values (1, 'a', 7, 1), (2, 'b', 1, 2), (3, 'c', 8, 3)",
        &[SUMS],
    )
    .await;
    raw.batch_execute("insert into public.c values (4, 'd', 2, 5000)")
        .await
        .unwrap();
    raw.batch_execute(
        "drop table public.c; \
         create table public.c (id int not null, tag text not null, pid int not null, \
             amount int, primary key (tag, id)); \
         create index on public.c (pid); \
         insert into public.c values (1, 'a', 1, 40), (5, 'e', 7, 60), (3, 'c', 8, 1000)",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// A 1-1 target is keyed by the source's key, so the resume refuses, naming
/// `DROP TRANSFORM`, and leaves the definition paused.
#[tokio::test]
async fn a_one_to_one_resume_onto_a_source_with_another_key_refuses() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        SOURCE,
        &["TRANSFORM c_copy FROM public.c SELECT amount AS amount"],
    )
    .await;
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    let err = trellis
        .apply("RESUME TRANSFORM c_copy")
        .await
        .expect_err("the target is keyed by the old key")
        .to_string();
    assert!(
        err.contains("is now (pid, id)") && err.contains("keyed by (id)"),
        "{err}"
    );
    assert!(err.contains("DROP TRANSFORM"), "{err}");
    assert_eq!(
        status(&trellis, "c_copy").await.status,
        TransformStatus::Paused
    );
}

/// A plain source converted to a partitioned one (the routine case): a
/// partitioned table's key must hold the partition key, so the conversion
/// widens it. The resume rebuilds onto the tree, and capture covers the
/// leaves.
const PARTITIONED: &str = "drop table public.c; \
     create table public.c (id int not null, pid int not null, amount int, \
         primary key (pid, id)) partition by list (pid); \
     create table public.c_7 partition of public.c for values in (7); \
     create table public.c_8 partition of public.c for values in (8); \
     create index on public.c (pid); \
     insert into public.c values (1, 7, 40), (2, 7, 60), (3, 8, 1000)";

#[tokio::test]
async fn an_aggregate_resumes_onto_its_source_converted_to_a_partitioned_table() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    raw.batch_execute("insert into public.c values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(PARTITIONED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    raw.batch_execute(
        "insert into public.c values (4, 8, 1), (5, 7, 2); delete from public.c where id = 1",
    )
    .await
    .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// An aggregate whose target holds 500 groups over 25,001 source rows when
/// its source is dropped and created again with two groups: the resume's
/// rebuild removes every group the new table lacks, so no old group survives
/// into the rebuilt target.
#[tokio::test]
async fn an_aggregate_of_many_groups_resumes_onto_a_wider_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.c (id int primary key, pid int not null, amount int); \
         create index on public.c (pid); \
         insert into public.c select i, i % 500, i from generate_series(1, 25001) i",
        &[SUMS],
    )
    .await;
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}
