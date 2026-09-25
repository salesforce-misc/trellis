//! Issue #469: every row a drain derives for a downstream transform carries
//! the `origin_lsn` of the source commit it traces back to, hop by hop, so
//! `converged_through` gates it only for tokens at or past that commit. A
//! change of unknown origin keeps its derived rows unknown, which gates every
//! token.
//!
//! Driven by hand through the engine's own seal, fold and apply path, with no
//! live intake: `orders -> hop_a -> hop_b -> hop_c`, all 1-1.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{create_definition, create_target_table, source_primary_key};
use trellis::staging::{StagedWatermark, apply, seal};

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

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

async fn seal_active_segment(client: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn drain(pool: &trellis::Pool, seg_seq: i64) {
    let watermark = StagedWatermark::saturated();
    while apply::drain_once(pool, seg_seq, "origin_test", 1, "wake", &watermark)
        .await
        .expect("drain_once")
        .is_some()
    {}
}

/// Stages one CDC insert for `orders` key `key`, as intake would, with
/// `origin_lsn` as given (`None` standing in for a row of unknown origin).
///
/// Stages at a made-up LSN on purpose (issue #512): intake stamps a change's
/// `origin_lsn` with its own commit LSN, and these tests assert on exact
/// origins, so the ring `lsn` follows the chosen origin. Only 1-1 targets
/// read these rows, and nothing here compares them with a recompute horizon.
async fn stage_insert(client: &Client, key: &str, price: &str, origin_lsn: Option<u64>) {
    let lsn = PgLsn::from(origin_lsn.unwrap_or(1));
    let origin = origin_lsn.map(PgLsn::from);
    let image = format!(r#"{{"id":"{key}","price":"{price}"}}"#);
    client
        .execute(
            "insert into seg_0 (src_table, key, op, lsn, new_image, origin_lsn, hop_gen) \
             values ('public.orders', $1, 'insert', $2, $3::text::jsonb, $4, 0)",
            &[&key, &lsn, &image, &origin],
        )
        .await
        .expect("stage insert");
}

/// The `origin_lsn` of every ring row staged for `src_table`, by key.
async fn staged_origins(client: &Client, src_table: &str) -> HashMap<String, Option<u64>> {
    let mut origins = HashMap::new();
    for slot in 0..4 {
        for row in client
            .query(
                &format!("select key, origin_lsn from seg_{slot} where src_table = $1"),
                &[&src_table],
            )
            .await
            .expect("read ring")
        {
            let origin: Option<PgLsn> = row.get(1);
            origins.insert(row.get(0), origin.map(u64::from));
        }
    }
    origins
}

#[tokio::test]
async fn derived_rows_carry_their_source_commits_origin_hop_by_hop() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute("create table public.orders (id integer primary key, price numeric)")
        .await
        .expect("create source");

    // Created while `orders` is empty, so no backfill row joins the ring.
    let pk = source_primary_key(&db.pool, "orders").await.expect("pk");
    for (target, source, columns) in [
        ("hop_a", "orders", ["id", "price"]),
        ("hop_b", "hop_a", ["id", "v"]),
        ("hop_c", "hop_b", ["id", "v"]),
    ] {
        let source_columns = numeric_columns(&columns);
        let value = if source == "orders" { "price" } else { "v" };
        let definition = create_definition(
            &db.pool,
            &format!("TRANSFORM {target} FROM {source} SELECT {value} AS v"),
            &source_columns,
        )
        .await
        .unwrap_or_else(|e| panic!("define {target}: {e}"));
        create_target_table(
            &db.pool,
            &definition.def,
            "public",
            &pk,
            &source_columns,
            &definition.def.source,
        )
        .await
        .unwrap_or_else(|e| panic!("create {target}: {e}"));
    }

    client
        .batch_execute("insert into public.orders values (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed source rows");
    // Key 1 has a known origin; key 2 an unknown one; key 3 folds a known
    // and an unknown row, and an unknown origin wins the fold.
    stage_insert(&client, "1", "10", Some(5_000)).await;
    stage_insert(&client, "2", "20", None).await;
    stage_insert(&client, "3", "30", Some(6_000)).await;
    stage_insert(&client, "3", "30", None).await;

    let expected = HashMap::from([
        ("1".to_string(), Some(5_000)),
        ("2".to_string(), None),
        ("3".to_string(), None),
    ]);

    let first = seal_active_segment(&mut client).await;
    drain(&db.pool, first).await;
    assert_eq!(
        staged_origins(&client, "public.hop_a").await,
        expected,
        "the first hop's rows carry their source rows' origins"
    );

    let second = seal_active_segment(&mut client).await;
    drain(&db.pool, second).await;
    assert_eq!(
        staged_origins(&client, "public.hop_b").await,
        expected,
        "and so do the second hop's"
    );
}
