//! Front-door integration test for issue #180: an **extinct** upstream
//! aggregate group must reduce (or, when it was the group's sole
//! contributor, remove) a chained downstream aggregate — reproduced here
//! with the *plainest* possible shape, a single-column `GROUP BY` at every
//! level:
//!
//! ```text
//! TRANSFORM sku_totals    FROM sales       GROUP BY sku SELECT sum(amount) AS total
//! TRANSFORM sku_totals_v2 FROM sku_totals  GROUP BY sku SELECT sum(total)  AS total2
//! ```
//!
//! The issue's own writeup is explicit that this bug is encoding-independent
//! — it reproduces identically for a plain single-column `GROUP BY` chain,
//! unaffected by the #171/#163 composite-key encoding fix
//! (`defs_aggregate_chained_composite_group_key.rs`'s own front door). This
//! file is that single-column pin: `sku_totals`' identity is a bare,
//! unencoded primary key (issue #103's shape, not #171's U+001F-joined
//! composite one), so it exercises a different `derive_group_key`/
//! `ddl::split_pk_key` code path than the composite test while hitting the
//! exact same root cause — before issue #180, deleting a group's only row
//! removed it upstream (`sku_totals`) but the downstream `Recompute` that
//! propagated it carried no image, so `sku_totals_v2`'s live re-fetch found
//! the row already gone and silently dropped the change instead of
//! subtracting its last-known contribution.
//!
//! `sku_totals_v2` deliberately groups by the exact same column
//! `sku_totals` does (an identity re-aggregation, not a coarser fan-in): a
//! single-column upstream `GROUP BY` key is, by construction, already a
//! distinct-per-group identity, so a *coarser* downstream regrouping needs a
//! second raw column riding along, which only a composite (or
//! relationship-path) upstream key can expose — see
//! `defs_aggregate_chained_composite_group_key.rs` for that shape. Here, an
//! extinct upstream group's downstream counterpart must itself go fully
//! extinct (its `SUM` of one vanished contributor drops to nothing), which
//! exercises the *other* branch of the fix: `apply_aggregate_target`'s own
//! extinction check (`probe_group_exists`) against the deleted row's decoded
//! image, not just a partial `SUM` decrement.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, install_definition};
use trellis::staging::apply;
use trellis::staging::{has_pending, retire_drained_segments};

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

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

/// Bare (no `.`) `name` qualified under [`DEFAULT_SCHEMA`] — see
/// `defs_aggregate_group_by_relationship.rs`'s helper of the same name.
fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
}

/// Stages one image-bearing (CDC-shaped) change into the active ring segment.
async fn stage_cdc(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let src_table = qualify_fixture_table(src_table);
    let table = active_seg_table(client).await;
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0)"
            ),
            &[&src_table, &key, &op, &lsn, &old_image, &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key:?} into {table} failed: {e}"));
}

/// Seals and drains repeatedly until nothing is pending anywhere in the ring.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "agg_chained_single_column_test",
            1,
            "trellis_agg_chained_single_column_test",
            watermark,
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

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

fn sales_columns() -> HashMap<String, ValueType> {
    columns(&[
        ("id", ValueType::Numeric),
        ("sku", ValueType::Text),
        ("amount", ValueType::Numeric),
    ])
}

fn sku_totals_columns() -> HashMap<String, ValueType> {
    columns(&[("sku", ValueType::Text), ("total", ValueType::Numeric)])
}

const SKU_TOTALS: &str = "TRANSFORM sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total";
const SKU_TOTALS_V2: &str =
    "TRANSFORM sku_totals_v2 FROM sku_totals GROUP BY sku SELECT sum(total) AS total2";

/// `sales` needs `REPLICA IDENTITY FULL` because it's an aggregate source
/// (the delta path needs the old image to locate the group a changed row is
/// leaving); `sku_totals` needs it for the same reason once `sku_totals_v2`
/// chains onto it, and can only be widened after `install_definition` has
/// created it.
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table sales ( \
                 id integer primary key, sku text, amount integer \
             ); \
             alter table sales replica identity full; \
             insert into sales (id, sku, amount) values \
               (1, 'a', 5), (2, 'a', 7), (3, 'b', 2), (4, 'c', 11)",
        )
        .await
        .expect("create + seed the single-column GROUP BY chain's schema");
}

/// `sku -> total`.
async fn sku_totals(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query("select sku, total::text from sku_totals", &[])
        .await
        .expect("read sku_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// `sku -> total2`.
async fn sku_totals_v2(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query("select sku, total2::text from sku_totals_v2", &[])
        .await
        .expect("read sku_totals_v2")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Installs both definitions and drains until both targets hold their
/// from-scratch backfilled state.
async fn install_the_chain(db: &testkit::TestDatabase, client: &mut Client) {
    install_definition(&db.pool, SKU_TOTALS, &sales_columns(), "public")
        .await
        .expect("install the single-column GROUP BY aggregate");
    drain_to_quiescence(&db.pool, client).await;

    client
        .batch_execute("alter table sku_totals replica identity full")
        .await
        .expect("widen sku_totals's replica identity");

    install_definition(&db.pool, SKU_TOTALS_V2, &sku_totals_columns(), "public")
        .await
        .expect("install the chained aggregate reading sku_totals");
    drain_to_quiescence(&db.pool, client).await;

    // Both backfills: a=12 (5+7), b=2, c=11; identity re-aggregation carries
    // the same values through sku_totals_v2.
    assert_eq!(
        sku_totals(client).await,
        HashMap::from([
            ("a".to_string(), Some("12".to_string())),
            ("b".to_string(), Some("2".to_string())),
            ("c".to_string(), Some("11".to_string())),
        ]),
        "from-scratch backfill of the single-column-key aggregate"
    );
    assert_eq!(
        sku_totals_v2(client).await,
        HashMap::from([
            ("a".to_string(), Some("12".to_string())),
            ("b".to_string(), Some("2".to_string())),
            ("c".to_string(), Some("11".to_string())),
        ]),
        "from-scratch backfill of the chained aggregate"
    );
}

/// Issue #180's exact repro, single-column-keyed: deleting `sku = 'b'`'s only
/// row makes that group extinct upstream (`sku_totals`), and the deletion
/// must propagate all the way through to `sku_totals_v2` — removing its own
/// `b` entry — rather than leaving `sku_totals_v2`'s stale `b: 2` forever.
/// A sibling live update to a *surviving* group (`a`'s amount grows by 3) and
/// a brand-new group (`d`) ride along in the same batch to confirm ordinary
/// propagation is unaffected by the fix.
#[tokio::test]
async fn an_extinct_single_column_group_reduces_the_chained_downstream_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    install_the_chain(&db, &mut client).await;

    // (w/o warehouse dimension here) sku 'b' goes extinct (row 3 deleted),
    // sku 'a' grows by 3 (row 1 updated 5 -> 8), and a brand-new sku 'd'
    // arrives — all in one batch.
    client
        .batch_execute(
            "update sales set amount = 8 where id = 1; \
             insert into sales (id, sku, amount) values (5, 'd', 9); \
             delete from sales where id = 3",
        )
        .await
        .expect("update a survivor, insert a new group, delete an extinct group's only row");
    stage_cdc(
        &client,
        "sales",
        "1",
        "update",
        Some(r#"{"id":"1","sku":"a","amount":"5"}"#),
        Some(r#"{"id":"1","sku":"a","amount":"8"}"#),
    )
    .await;
    stage_cdc(
        &client,
        "sales",
        "5",
        "insert",
        None,
        Some(r#"{"id":"5","sku":"d","amount":"9"}"#),
    )
    .await;
    stage_cdc(
        &client,
        "sales",
        "3",
        "delete",
        Some(r#"{"id":"3","sku":"b","amount":"2"}"#),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        sku_totals(&client).await,
        HashMap::from([
            ("a".to_string(), Some("15".to_string())),
            ("c".to_string(), Some("11".to_string())),
            ("d".to_string(), Some("9".to_string())),
        ]),
        "'b' is extinct upstream, 'a' grew by 3, 'd' is new"
    );
    assert_eq!(
        sku_totals_v2(&client).await,
        HashMap::from([
            ("a".to_string(), Some("15".to_string())),
            ("c".to_string(), Some("11".to_string())),
            ("d".to_string(), Some("9".to_string())),
        ]),
        "issue #180: the extinct 'b' group's downstream counterpart must be \
         removed too (not left stale at total2 = 2), while the ordinary \
         update and the brand-new group still propagate normally"
    );
}
