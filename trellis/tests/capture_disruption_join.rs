//! The table-gone check of #707/#999 for a `JOIN`'s joined table and a
//! relationship's to-side. Its own test binary: a `JOIN` needs the
//! group-basis knob, which is process-wide and would change every aggregate
//! build in `capture_disruption.rs`.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

const SCHEMA: &str = "trellis";

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

async fn definer(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer")
}

/// One whole staging-worker pass: capture, markers, discharge.
async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(
        raw,
        pool,
        SCHEMA,
        "capture_disruption_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

/// The capture half of one pass only.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = reconcile::reconcile(
        raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(2),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "capture_disruption_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_disruption_test",
            1,
            "trellis_capture_missed_ddl_test",
            &watermark,
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

async fn run_backfill_chunks(pool: &trellis::Pool) {
    use trellis::defs::chunk_queue;
    const CLAIMED_BY: &str = "capture_disruption_backfill";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            trellis::staging::build::settle_builds(pool).await;
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                CLAIMED_BY,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

/// Every definition's status, by bare target name.
async fn statuses(raw: &Client) -> BTreeMap<String, TransformStatus> {
    raw.query(
        "select split_part(target_table, '.', 2), status from transform_definitions",
        &[],
    )
    .await
    .expect("read statuses")
    .into_iter()
    .map(|row| {
        (
            row.get(0),
            TransformStatus::from_persisted(row.get(1)).expect("known status"),
        )
    })
    .collect()
}

/// Alternates staging-worker passes, builds and drains until every
/// definition is live: a bounded number of explicit steps.
async fn bring_live(raw: &mut Client, pool: &trellis::Pool) {
    for _ in 0..10 {
        full_pass(raw, pool).await;
        run_backfill_chunks(pool).await;
        drain_to_quiescence(pool, raw).await;
        let statuses = statuses(raw).await;
        if statuses.values().all(|s| *s == TransformStatus::Live) {
            return;
        }
    }
    panic!("not every definition went live: {:?}", statuses(raw).await);
}

async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

/// The table-gone case per reader kind: a `JOIN`'s joined table and a
/// relationship's to-side pause their readers (the 1-1 and aggregate sources
/// are the matrix's). A `JOIN`'s joined table once panicked the pass (#1177).
#[tokio::test]
async fn a_gone_joined_table_or_to_side_pauses_its_readers() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.ja (id int primary key, k varchar(5), x int); \
         create table public.jb (id int primary key, k varchar(5), y int); \
         create index on public.ja (k); create index on public.jb (k); \
         create table public.rc (id int primary key, pid int, amount int); \
         create table public.rp (id int primary key, name text); \
         insert into public.rp select i, 'p' || i from generate_series(1, 3) i; \
         insert into public.rc select i, 1 + i % 3, i from generate_series(1, 9) i;",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in [
        "TRANSFORM jl FROM public.ja LEFT JOIN public.jb ON ja.k = jb.k \
         GROUP BY ja.k SELECT ja.k AS k, COUNT(*) AS n, SUM(jb.y) AS sy",
        "RELATIONSHIP parent FROM rc.pid TO rp.id",
        "TRANSFORM rc_named FROM public.rc SELECT amount AS amount, parent.name AS name",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    app.batch_execute("drop table public.jb; drop table public.rp")
        .await
        .expect("drop the joined table and the to-side");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    for target in ["jl", "rc_named"] {
        let status = trellis.status(target).await.unwrap().unwrap();
        assert_eq!(status.status, TransformStatus::Paused, "{target}");
        let error = status.capture_failure.expect("a reason").error;
        assert!(error.contains("no longer exists"), "{target}: {error}");
    }
}

/// How many `capture_triggers`, `capture_baselines` and `partition_sources`
/// rows `table` has.
async fn record_counts(raw: &Client, table: &str) -> (i64, i64, i64) {
    let mut counts = Vec::new();
    for record in ["capture_triggers", "capture_baselines", "partition_sources"] {
        let sql = format!("select count(*) from {record} where table_name = '{table}'");
        counts.push(
            raw.query_one(&sql, &[])
                .await
                .expect("count")
                .get::<_, i64>(0),
        );
    }
    (counts[0], counts[1], counts[2])
}

// #1217
/// Dropping the last reader of a `JOIN`'s joined table, or of a
/// relationship's to-side, forgets that table's records; a table another
/// definition still reads keeps them.
#[tokio::test]
async fn dropping_the_last_reader_of_a_joined_table_or_to_side_forgets_its_records() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.fa (id int primary key, k varchar(5), x int); \
         create table public.fb (id int primary key, k varchar(5), y int); \
         create table public.fc (id int primary key, pid int, amount int); \
         create table public.fp (id int primary key, name text); \
         insert into public.fp select i, 'p' || i from generate_series(1, 3) i; \
         insert into public.fc select i, 1 + i % 3, i from generate_series(1, 9) i;",
    )
    .await
    .unwrap();
    let trellis = definer(db.dsn()).await;
    for text in [
        "TRANSFORM fj FROM public.fa LEFT JOIN public.fb ON fa.k = fb.k \
         GROUP BY fa.k SELECT fa.k AS k, COUNT(*) AS n, SUM(fb.y) AS sy",
        "RELATIONSHIP parent FROM fc.pid TO fp.id",
        "TRANSFORM fc_named FROM public.fc SELECT amount AS amount, parent.name AS name",
        "TRANSFORM fc_plain FROM public.fc SELECT amount AS amount",
    ] {
        trellis.apply(text).await.expect(text);
    }
    capture_pass(&mut raw, &db.pool).await;
    for t in ["public.fa", "public.fb", "public.fc", "public.fp"] {
        assert!(record_counts(&raw, t).await.0 > 0, "{t} is recorded");
    }

    trellis.apply("PAUSE TRANSFORM fj").await.unwrap();
    trellis.apply("DROP TRANSFORM fj").await.unwrap();
    for t in ["public.fa", "public.fb"] {
        assert_eq!(record_counts(&raw, t).await, (0, 0, 0), "{t}");
    }
    trellis.apply("PAUSE TRANSFORM fc_named").await.unwrap();
    trellis.apply("DROP TRANSFORM fc_named").await.unwrap();
    // A reader of `fc` still reaches `fp` through the relationship.
    assert!(record_counts(&raw, "public.fc").await.0 > 0);
    assert!(record_counts(&raw, "public.fp").await.0 > 0);
    trellis.apply("DROP RELATIONSHIP fc.parent").await.unwrap();
    assert!(
        record_counts(&raw, "public.fc").await.0 > 0,
        "fc_plain reads it"
    );
    assert_eq!(record_counts(&raw, "public.fp").await, (0, 0, 0));
}
