//! A logical restore (#1205) of an aggregate on the ledger path, the one an
//! aggregate takes without the group-basis knob: its ledger holds the
//! original cluster's positions, so after a restore into a cluster behind the
//! original's, the capture pass pauses it and `RESUME` resets the ledger
//! before the rebuild. Afterwards an update, a delete and a group move of keys
//! that existed before the dump apply. Its own test binary, because the
//! group-basis knob is process-wide (`logical_restore.rs` turns it on).
//! Nothing polls for convergence (#297); every step is an explicit call.

use std::collections::BTreeMap;
use std::process::Command;
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

async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(raw, pool, SCHEMA, "lr_agg_wake", Duration::from_secs(2))
        .await
        .expect("reconcile pass");
}

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
        seal::seal_phase2(client, outcome.sealed_seg_seq, "lr_agg_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "lr_agg_test",
            1,
            "trellis_lr_agg_test",
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
    const CLAIMED_BY: &str = "lr_agg_backfill";
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

async fn bring_live(raw: &mut Client, pool: &trellis::Pool) {
    for _ in 0..10 {
        full_pass(raw, pool).await;
        run_backfill_chunks(pool).await;
        drain_to_quiescence(pool, raw).await;
        if statuses(raw)
            .await
            .values()
            .all(|s| *s == TransformStatus::Live)
        {
            return;
        }
    }
    panic!("the aggregate did not go live: {:?}", statuses(raw).await);
}

async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[]).await.expect(sql).get(0)
}

fn run(command: &mut Command) {
    let out = command.output().expect("run a client tool");
    assert!(
        out.status.success(),
        "{command:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

async fn assert_oracle(raw: &Client, when: &str) {
    assert_eq!(
        rows(
            raw,
            "select grp::text, s::text, n::text from public.tot order by 1"
        )
        .await,
        rows(
            raw,
            "select grp::text, sum(amt)::text, count(*)::text from public.g group by grp \
             order by 1"
        )
        .await,
        "{when}: tot diverged from its oracle"
    );
}

#[tokio::test]
async fn a_ledger_aggregate_restored_behind_the_original_resumes_and_applies_later_writes() {
    trellis::dev::staging::set_group_basis(false);
    let a = TestCluster::start();
    let db = a.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.g (id int primary key, grp int not null, amt int); \
         insert into public.g select i, i % 3, i from generate_series(1, 9) i",
    )
    .await
    .expect("the source");
    definer(db.dsn())
        .await
        .apply(
            "TRANSFORM tot FROM public.g GROUP BY grp SELECT grp AS grp, SUM(amt) AS s, \
             COUNT(*) AS n",
        )
        .await
        .expect("define");
    bring_live(&mut raw, &db.pool).await;
    // Positions of an advanced cluster in the ledger.
    raw.batch_execute(
        "select pg_logical_emit_message(true, 'pad', repeat('x', 1000000)) \
         from generate_series(1, 24)",
    )
    .await
    .expect("advance the WAL");
    raw.batch_execute(
        "do $$ begin for i in 1..3000 loop perform pg_current_xact_id(); commit; end loop; end $$",
    )
    .await
    .expect("advance the counters");
    raw.batch_execute("update public.g set amt = amt + 100")
        .await
        .expect("changes");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_oracle(&raw, "the original").await;
    let dump = a.root().join("ledger_aggregate.dump");
    run(Command::new("pg_dump")
        .args(["-Fc", "-f"])
        .arg(&dump)
        .arg(db.dsn()));

    let b = TestCluster::start();
    let restored = b.create_empty_database().await;
    run(Command::new("pg_restore")
        .args(["--no-owner", "-d", restored.dsn()])
        .arg(&dump));
    let mut raw = connect(restored.dsn()).await;
    let trellis = definer(restored.dsn()).await;
    assert!(
        count(
            &raw,
            "select count(*) from public.tot__ledger \
             where __applied_lsn > pg_current_wal_insert_lsn()"
        )
        .await
            > 0,
        "the restore is behind the ledger's positions"
    );

    capture_pass(&mut raw, &restored.pool).await;
    assert_eq!(statuses(&raw).await["tot"], TransformStatus::Paused);
    let failure = trellis
        .status("tot")
        .await
        .expect("status")
        .expect("defined")
        .capture_failure
        .expect("a capture_failure");
    assert!(
        failure.error.contains("restored from a logical dump"),
        "{}",
        failure.error
    );

    trellis.apply("RESUME TRANSFORM tot").await.expect("resume");
    bring_live(&mut raw, &restored.pool).await;
    assert_oracle(&raw, "after the resume").await;
    raw.batch_execute(
        "update public.g set amt = 1000 where id = 1; \
         delete from public.g where id = 2; \
         update public.g set grp = 7 where id = 3",
    )
    .await
    .expect("writes");
    drain_to_quiescence(&restored.pool, &mut raw).await;
    assert_oracle(&raw, "after writes to keys that existed before the dump").await;
}
