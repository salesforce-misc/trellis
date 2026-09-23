//! Repro: a 1-1 target's stored primary key must be the source row's *raw*
//! primary-key text, byte-for-byte — never the staging ring's encoded key
//! text.
//!
//! No live `Client` (issue #301): each source change is mirrored by the CDC
//! row intake would stage for it, staged by hand and drained through the
//! engine's own apply path, so nothing here waits on a pipeline under a
//! wall-clock budget. The staged key is the raw key text, which is what
//! intake stages for a not-null primary key. That half, intake's own key
//! encoding, is pinned directly by `intake::tests`'
//! `extract_key_keeps_a_control_character_in_a_key_value_verbatim`
//! (`trellis/src/intake/mod.rs`). This file covers the apply half: the live
//! re-fetch, the target write, and the update/delete lookups all have to
//! agree on that raw text.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::Pool;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{create_definition, create_target_table};
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

/// Stages one CDC change for `orders` into the active ring segment, shaped
/// the way intake stages it: the raw key text, and text-valued JSON images.
async fn stage_cdc(
    client: &Client,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let active: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    client
        .execute(
            &format!(
                "insert into seg_{active} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0)"
            ),
            &[
                &format!("{DEFAULT_SCHEMA}.orders"),
                &key,
                &op,
                &PgLsn::from(1u64),
                &old_image,
                &new_image,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key:?}: {e}"));
}

/// Seals and drains through the engine's own apply path until nothing is
/// pending: the hand-driven stand-in for a running `Client`'s drain workers.
async fn drain_to_quiescence(pool: &Pool, client: &mut Client) {
    // No live `Intake` stages anything here, so there is no real staged
    // watermark to hold apply back. A saturated one never does.
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "control_char_pk_test",
            1,
            "trellis_control_char_pk_test",
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
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

async fn target_count(raw: &Client) -> i64 {
    raw.query_one("select count(*) from totals", &[])
        .await
        .expect("count target")
        .get(0)
}

fn totals_def() -> TransformDef {
    TransformDef {
        target: "totals".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("a".to_string())),
                rhs: Box::new(Expr::Column("b".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

async fn setup(pool: &Pool, raw: &Client) {
    raw.batch_execute("create table orders (id text primary key, a numeric, b numeric)")
        .await
        .expect("create source table");

    let source_columns = HashMap::from([
        ("id".to_string(), ValueType::Text),
        ("a".to_string(), ValueType::Numeric),
        ("b".to_string(), ValueType::Numeric),
    ]);
    create_definition(
        pool,
        "TRANSFORM totals FROM orders SELECT a + b AS total",
        &source_columns,
    )
    .await
    .expect("create definition");

    let pk = trellis::defs::source_primary_key(pool, "orders")
        .await
        .expect("introspect source primary key");
    create_target_table(
        pool,
        &totals_def(),
        "public",
        &pk,
        &source_columns,
        &totals_def().source,
    )
    .await
    .expect("create target table");
}

/// A `text` primary key whose value genuinely contains a U+0001 (SOH) must
/// land in the 1-1 target verbatim: the incremental apply path binds the
/// ring's key text straight in as the target's literal PK value, while
/// backfill (`select {pk} from {source}`), quarantine's resume write-back
/// (`where {pk}::text = $2`) and the oracle all use the *raw* column value.
/// If the key encoding escapes the value, those paths diverge and the row is
/// duplicated/orphaned.
#[tokio::test]
async fn a_one_to_one_target_stores_the_raw_source_pk_even_with_a_control_character() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;

    setup(&db.pool, &raw).await;

    let soh_id = "a\u{1}b".to_string();
    let plain_id = "plain".to_string();
    raw.execute(
        "insert into orders (id, a, b) values ($1, 1.00, 2.00), ($2, 3.00, 4.00)",
        &[&soh_id, &plain_id],
    )
    .await
    .expect("insert source rows");
    // JSON's `\u0001` escape decodes to the same U+0001 `soh_id` holds.
    stage_cdc(
        &raw,
        &soh_id,
        "insert",
        None,
        Some(r#"{"id":"a\u0001b","a":"1.00","b":"2.00"}"#),
    )
    .await;
    stage_cdc(
        &raw,
        &plain_id,
        "insert",
        None,
        Some(r#"{"id":"plain","a":"3.00","b":"4.00"}"#),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    let ids: Vec<String> = raw
        .query("select id from totals order by id", &[])
        .await
        .expect("read target ids")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    let mut expected = vec![soh_id.clone(), plain_id.clone()];
    expected.sort();
    assert_eq!(
        ids, expected,
        "the target's stored primary keys must equal the source's raw primary keys \
         (a doubled U+0001 here means the ring's encoded key text leaked into the \
         target's literal PK column)"
    );

    // And the derived row must be *updatable* through the same path: an
    // update that lands under a differently-encoded key would insert a
    // second row instead of updating the first.
    raw.execute("update orders set a = 10.00 where id = $1", &[&soh_id])
        .await
        .expect("update source row");
    stage_cdc(
        &raw,
        &soh_id,
        "update",
        None,
        Some(r#"{"id":"a\u0001b","a":"10.00","b":"2.00"}"#),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    let total: Option<String> = raw
        .query_opt("select total::text from totals where id = $1", &[&soh_id])
        .await
        .expect("read target row")
        .map(|r| r.get(0));
    assert_eq!(
        total.as_deref(),
        Some("12.00"),
        "the update must land on the row the insert wrote"
    );
    assert_eq!(
        target_count(&raw).await,
        2,
        "the update must not have inserted a duplicate row"
    );

    // A delete must remove the row it originally wrote, not miss it.
    raw.execute("delete from orders where id = $1", &[&soh_id])
        .await
        .expect("delete source row");
    stage_cdc(&raw, &soh_id, "delete", Some(r#"{"id":"a\u0001b"}"#), None).await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    let ids: Vec<String> = raw
        .query("select id from totals", &[])
        .await
        .expect("read target ids")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        ids,
        vec![plain_id],
        "the delete must remove exactly the row the insert wrote"
    );
}
