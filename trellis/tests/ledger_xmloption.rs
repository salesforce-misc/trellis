//! A database-level `xmloption = document` doesn't fail a ledger page over
//! an `xml` fragment (found in #623 D3b's review).
//!
//! The ledger casts an image's text into its contribution columns through
//! `jsonb_populate_record` (`staging::ledger`), so an `xml` contribution
//! (`COUNT(x)` over an `xml` column) goes through `xml_in`, which parses
//! under the session's `xmloption`. Under `document`, content a writer
//! stored under `content` (a fragment such as `a<b/>`) doesn't parse, and
//! the whole page fails. `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` pins
//! `xmloption` to `content`, which accepts both.
//!
//! Stepped by hand: the writes come from a separate session, then explicit
//! staging-worker passes and seal/drain rounds. Nothing polls (#297).

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::sql::pinned_output_settings;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

const SCHEMA: &str = "trellis";

/// A connection pinned the way the staging worker's is: Trellis's schema
/// first, plus every pinned output setting.
async fn connect_pinned(dsn: &str) -> Client {
    let client = connect(dsn).await;
    client
        .batch_execute(&format!(
            "set search_path to {SCHEMA}, public; {}",
            pinned_output_settings().join("; ")
        ))
        .await
        .expect("pin the session");
    client
}

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// One whole staging-worker pass: capture, markers, discharge.
async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(
        raw,
        pool,
        SCHEMA,
        "ledger_xmloption_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

/// Seals and drains through the engine's own apply path until nothing is
/// pending anywhere in the ring (as in `capture_schema_change.rs`).
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "ledger_xmloption_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "ledger_xmloption_test",
            1,
            "trellis_ledger_xmloption_test",
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

/// Claims, runs and finishes every pending backfill chunk (as in
/// `capture_schema_change.rs`).
async fn run_backfill_chunks(pool: &trellis::Pool) {
    use trellis::defs::chunk_queue;
    const CLAIMED_BY: &str = "ledger_xmloption_backfill";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
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

async fn status(raw: &Client, target: &str) -> TransformStatus {
    let text: String = raw
        .query_one(
            "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0);
    TransformStatus::from_persisted(&text).expect("known status")
}

async fn counts(raw: &Client, sql: &str) -> Vec<(i32, i64)> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[tokio::test]
async fn an_xml_fragment_applies_through_the_ledger_under_a_document_xmloption() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    {
        let setup = connect(db.dsn()).await;
        setup
            .batch_execute(&format!(
                "create table public.x (id int primary key, g int, doc xml); \
                 insert into public.x values (1, 1, 'a<b/>'), (2, 1, '<c/>'), (3, 2, null); \
                 alter database \"{}\" set xmloption to document",
                db.name()
            ))
            .await
            .expect("seed, then make document the database's xmloption");
    }
    // A session that pins nothing now refuses a fragment: the hazard is real.
    let unpinned = connect(db.dsn()).await;
    assert!(
        unpinned.batch_execute("select 'a<b/>'::xml").await.is_err(),
        "a fragment must not parse under the database's xmloption, or this test proves nothing"
    );

    // Everything Trellis opens from here on starts under `document`:
    // `db.pool`'s connections predate the `ALTER DATABASE`, so a fresh pool.
    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let pool = trellis::Pool::new(&config).expect("build a pool");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect a definer");
    trellis
        .apply("TRANSFORM tx FROM public.x GROUP BY g SELECT g AS g, COUNT(doc) AS n")
        .await
        .expect("define");
    let mut raw = connect_pinned(db.dsn()).await;
    for _ in 0..8 {
        full_pass(&mut raw, &pool).await;
        run_backfill_chunks(&pool).await;
        drain_to_quiescence(&pool, &mut raw).await;
        if status(&raw, "tx").await == TransformStatus::Live {
            break;
        }
    }
    assert_eq!(status(&raw, "tx").await, TransformStatus::Live);

    // The application stores fragments under its own `content` session.
    let writer = connect(db.dsn()).await;
    writer
        .batch_execute(
            "set xmloption to content; \
             insert into public.x values (4, 2, 'd<e/>f'), (5, 1, 'text only'); \
             update public.x set doc = 'g<h/>' where id = 3; \
             update public.x set g = 2 where id = 1",
        )
        .await
        .expect("the application's writes");
    drain_to_quiescence(&pool, &mut raw).await;

    assert_eq!(
        counts(&raw, "select g, n from public.tx where n <> 0 order by g").await,
        counts(
            &raw,
            "select g, count(doc) from public.x group by g having count(doc) <> 0 order by g"
        )
        .await,
        "tx converged over the fragments"
    );
    assert_eq!(status(&raw, "tx").await, TransformStatus::Live);
    let unhealthy: i64 = raw
        .query_one(
            "select (select count(*) from poison) + (select count(*) from column_failures)",
            &[],
        )
        .await
        .expect("read failures")
        .get(0);
    assert_eq!(unhealthy, 0, "no page failed");
}
