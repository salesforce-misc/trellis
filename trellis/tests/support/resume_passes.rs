//! A staging worker stepped by hand for the resume tests of #1324: whole
//! passes, the capture half of one, backfill jobs and drains, each called
//! directly. Nothing polls for convergence (#297).

#![allow(dead_code)]

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

pub const SCHEMA: &str = "trellis";

pub async fn connect(dsn: &str) -> Client {
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

pub async fn definer(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer")
}

/// One whole staging-worker pass: capture, markers, discharge.
pub async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(
        raw,
        pool,
        SCHEMA,
        "resume_key_change_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

/// The capture half of one pass only.
pub async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) {
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

pub async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "resume_key_change_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "resume_key_change_test",
            1,
            "resume_key_change_wake",
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

pub async fn run_backfill_chunks(pool: &trellis::Pool) {
    use trellis::defs::chunk_queue;
    const CLAIMED_BY: &str = "resume_key_change_backfill";
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

pub async fn status(trellis: &Trellis, target: &str) -> trellis::DefinitionStatus {
    trellis
        .status(target)
        .await
        .expect("status")
        .unwrap_or_else(|| panic!("{target} is registered"))
}

/// Alternates staging-worker passes, builds and drains until `targets` are
/// live: a bounded number of explicit steps.
pub async fn bring_live(
    raw: &mut Client,
    pool: &trellis::Pool,
    trellis: &Trellis,
    targets: &[&str],
) {
    for _ in 0..10 {
        full_pass(raw, pool).await;
        run_backfill_chunks(pool).await;
        drain_to_quiescence(pool, raw).await;
        let mut all = true;
        for target in targets {
            let reported = status(trellis, target).await;
            assert_eq!(reported.backfill_failure, None, "{target}");
            assert_eq!(reported.drain_failure, None, "{target}");
            assert_eq!(reported.held_keys, None, "{target}");
            all &= reported.status == TransformStatus::Live;
        }
        if all {
            return;
        }
    }
    panic!("not every definition went live");
}

pub async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

/// The source every case starts from: `public.c (id pk, pid, amount)`, 6
/// rows, indexed by `pid`.
pub const SOURCE: &str = "create table public.c (id int primary key, pid int not null, amount int); \
     create index on public.c (pid); \
     insert into public.c select i, i % 3, i from generate_series(1, 6) i";

/// `public.c` recreated with the two-column key `(pid, id)`, holding other
/// rows.
pub const RECREATED: &str = "drop table public.c; \
     create table public.c (id int not null, pid int not null, amount int, \
         primary key (pid, id)); \
     create index on public.c (pid); \
     insert into public.c values (1, 7, 40), (2, 7, 60), (3, 8, 1000)";

/// A database with `source_ddl` run and `definitions` live over it.
pub async fn setup(
    cluster: &TestCluster,
    source_ddl: &str,
    definitions: &[&str],
) -> (testkit::TestDatabase, Client, Trellis) {
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(source_ddl).await.expect("the sources");
    let trellis = definer(db.dsn()).await;
    for text in definitions {
        trellis.apply(text).await.expect(text);
    }
    let targets: Vec<&str> = definitions
        .iter()
        .map(|t| t.split_whitespace().nth(1).expect("a target"))
        .collect();
    bring_live(&mut raw, &db.pool, &trellis, &targets).await;
    (db, raw, trellis)
}

/// Asserts the ring holds a change not yet drained: the one a case staged
/// under the old key before the drop.
pub async fn assert_staged(raw: &Client) {
    assert!(
        trellis::staging::has_pending(raw)
            .await
            .expect("has_pending"),
        "a change is staged"
    );
}
