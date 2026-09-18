//! Front-door integration tests for issue #171: a **live incremental**
//! change that propagates downstream into a chained aggregate whose upstream
//! aggregate has a **multi-column `GROUP BY`**.
//!
//! ```text
//! TRANSFORM stock_totals    FROM inventory     GROUP BY warehouse, sku SELECT sum(qty)       AS total_qty
//! TRANSFORM stock_totals_v2 FROM stock_totals  GROUP BY warehouse      SELECT sum(total_qty) AS total_qty
//! ```
//!
//! No relationship is involved — this is the plainest possible two-column
//! `GROUP BY` chained onto by a second aggregate, exactly the issue's own
//! repro shape.
//!
//! `stock_totals`' composite `(warehouse, sku)` identity is what makes this
//! interesting: when a group's row changes, `apply_and_mark_drained_many`'s
//! downstream-propagation step stages a `Recompute` against `stock_totals`
//! keyed by `apply_aggregate::derive_group_key`'s encoded text, and
//! `stock_totals_v2`'s own live re-fetch (`apply::read_live_rows_batch` →
//! `ddl::split_pk_key`) decodes it as `stock_totals`' real (composite)
//! primary key. Before the fix those two encodings disagreed —
//! `derive_group_key` emitted a locally-invented length-prefixed form
//! (`"2:w1" + "1:a"`), the decoder split on U+001F — so the drain failed
//! outright with `DdlError::MalformedCompositeKey { key: "2:w11:a",
//! expected_arity: 2, actual_arity: 1 }`, taking the whole batch with it.
//! Issue #103 had already fixed the single-column twin of this bug (a
//! one-column `GROUP BY` emits its bare value, matching a single-column PK);
//! #171 is the same treatment for the composite case.
//!
//! Backfilling both definitions from scratch was never broken (a direct
//! table scan builds no per-row key text at all), so every test here
//! deliberately drives a *live* change after both definitions are already
//! installed and converged. The harness (hand-staged CDC rows + seal/drain
//! to quiescence) mirrors `defs_aggregate_group_by_relationship.rs`, whose
//! own chaining test documented this bug but deliberately stayed off the
//! broken path.

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
///
/// The `expect("drain_once")` here is this file's actual regression pin for
/// issue #171: the pre-fix failure mode is a hard `ApplyError` (a
/// `DdlError::MalformedCompositeKey` raised while decoding the downstream
/// `Recompute`'s key), so a regression panics right here rather than merely
/// producing a wrong number further down.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "agg_chained_composite_test",
            1,
            "trellis_agg_chained_composite_test",
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

fn inventory_columns() -> HashMap<String, ValueType> {
    columns(&[
        ("id", ValueType::Numeric),
        ("warehouse", ValueType::Text),
        ("sku", ValueType::Text),
        ("qty", ValueType::Numeric),
    ])
}

fn stock_totals_columns() -> HashMap<String, ValueType> {
    columns(&[
        ("warehouse", ValueType::Text),
        ("sku", ValueType::Text),
        ("total_qty", ValueType::Numeric),
    ])
}

const STOCK_TOTALS: &str = "TRANSFORM stock_totals FROM inventory GROUP BY warehouse, sku \
     SELECT sum(qty) AS total_qty";
const STOCK_TOTALS_V2: &str = "TRANSFORM stock_totals_v2 FROM stock_totals GROUP BY warehouse \
     SELECT sum(total_qty) AS total_qty";

/// The issue's own schema. `inventory` needs `REPLICA IDENTITY FULL` because
/// it's an aggregate source (the delta path needs the old image to locate the
/// group a changed row is leaving); `stock_totals` needs it for the same
/// reason once `stock_totals_v2` chains onto it, and can only be widened
/// after `install_definition` has created it.
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table inventory ( \
                 id integer primary key, warehouse text, sku text, qty integer \
             ); \
             alter table inventory replica identity full; \
             insert into inventory (id, warehouse, sku, qty) values \
               (1, 'w1', 'a', 5), (2, 'w1', 'a', 7), (3, 'w1', 'b', 2), \
               (4, 'w2', 'a', 11)",
        )
        .await
        .expect("create + seed issue #171's schema");
}

/// `(warehouse, sku) -> total_qty`.
async fn stock_totals(client: &Client) -> HashMap<(String, String), Option<String>> {
    client
        .query(
            "select warehouse, sku, total_qty::text from stock_totals",
            &[],
        )
        .await
        .expect("read stock_totals")
        .into_iter()
        .map(|r| ((r.get(0), r.get(1)), r.get(2)))
        .collect()
}

/// `warehouse -> total_qty`.
async fn stock_totals_v2(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query(
            "select warehouse, total_qty::text from stock_totals_v2",
            &[],
        )
        .await
        .expect("read stock_totals_v2")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Installs both definitions and drains until both targets hold their
/// from-scratch backfilled state (the part that always worked).
async fn install_the_chain(db: &testkit::TestDatabase, client: &mut Client) {
    install_definition(&db.pool, STOCK_TOTALS, &inventory_columns(), "public")
        .await
        .expect("install the multi-column GROUP BY aggregate");
    drain_to_quiescence(&db.pool, client).await;

    client
        .batch_execute("alter table stock_totals replica identity full")
        .await
        .expect("widen stock_totals's replica identity");

    install_definition(&db.pool, STOCK_TOTALS_V2, &stock_totals_columns(), "public")
        .await
        .expect("install the chained aggregate reading stock_totals");
    drain_to_quiescence(&db.pool, client).await;

    // Both backfills: (w1,a)=12, (w1,b)=2, (w2,a)=11; w1=14, w2=11.
    assert_eq!(
        stock_totals(client).await,
        HashMap::from([
            (("w1".to_string(), "a".to_string()), Some("12".to_string())),
            (("w1".to_string(), "b".to_string()), Some("2".to_string())),
            (("w2".to_string(), "a".to_string()), Some("11".to_string())),
        ]),
        "from-scratch backfill of the composite-key aggregate"
    );
    assert_eq!(
        stock_totals_v2(client).await,
        HashMap::from([
            ("w1".to_string(), Some("14".to_string())),
            ("w2".to_string(), Some("11".to_string())),
        ]),
        "from-scratch backfill of the chained aggregate"
    );
}

/// Issue #171's exact repro: a live `UPDATE` on `inventory` that changes one
/// `stock_totals` group's total must propagate all the way through to
/// `stock_totals_v2` — instead of failing the drain with
/// `DdlError::MalformedCompositeKey { source_table: "stock_totals", key:
/// "2:w11:a", expected_arity: 2, actual_arity: 1 }`.
#[tokio::test]
async fn a_live_update_propagates_through_a_composite_group_key_into_a_chained_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    install_the_chain(&db, &mut client).await;

    // Row 2's qty: 7 -> 10, so (w1, a) goes 12 -> 15 and w1 goes 14 -> 17.
    client
        .execute("update inventory set qty = 10 where id = 2", &[])
        .await
        .expect("update inventory row 2");
    stage_cdc(
        &client,
        "inventory",
        "2",
        "update",
        Some(r#"{"id":"2","warehouse":"w1","sku":"a","qty":"7"}"#),
        Some(r#"{"id":"2","warehouse":"w1","sku":"a","qty":"10"}"#),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        stock_totals(&client).await.get(&("w1".into(), "a".into())),
        Some(&Some("15".to_string())),
        "the composite-key group's own live delta"
    );
    assert_eq!(
        stock_totals_v2(&client).await,
        HashMap::from([
            ("w1".to_string(), Some("17".to_string())),
            ("w2".to_string(), Some("11".to_string())),
        ]),
        "the live change must propagate downstream through the composite \
         group key (issue #171)"
    );
}

/// The other two live shapes a composite-key group can take downstream, in
/// one batch: a brand-new group (an `INSERT` under a `sku` that didn't exist
/// yet) and a group that goes extinct (a `DELETE` of its only row, which
/// stages a *deleted* group key — the other half of
/// `apply_aggregate_target`'s `written`/`deleted` result, and so the other
/// half of what the downstream-propagation step encodes).
///
/// Both now propagate. Before issue #180, the extinct one did **not**: a
/// downstream `Recompute` carried no image, so `stock_totals_v2` re-read the
/// key live, found the row already gone, and had no way to know which of
/// *its* groups that vanished row used to contribute to, so the change was
/// dropped — `staging::apply_aggregate`'s module doc comment ("Image-less
/// changes, and issue #180's fix for one producer of them") has the full
/// story. That gap was entirely independent of issue #171's key *encoding*
/// (it reproduced identically for a single-column `GROUP BY` chain, whose
/// encoding #103 already fixed — see
/// `defs_aggregate_chained_single_column_group_key.rs`) and closing it
/// needed the deleted group's old values threaded through the propagation
/// step, not a different key format: `apply_aggregate_target`'s
/// `delete_group_row`/`apply_forced_groups_bulk` now capture the extinct
/// group's pre-delete row (`RETURNING to_jsonb(t.*)`), and downstream
/// propagation stages it as a real image-bearing delete instead of an
/// image-less `Recompute`. #171's own narrower fix is still exercised here
/// too: the extinct group's key still needs to *decode* correctly (rather
/// than failing the whole batch with `MalformedCompositeKey`) for either the
/// old gap or this fix to be reachable at all — which is why the sibling
/// `(w2, c)` insert in this same batch lands regardless.
#[tokio::test]
async fn a_live_insert_and_an_extinct_composite_group_both_propagate_downstream() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    install_the_chain(&db, &mut client).await;

    // A brand-new (w2, c) group worth 4, and the extinction of (w1, b)
    // (row 3 was its only row), in one batch.
    client
        .batch_execute(
            "insert into inventory (id, warehouse, sku, qty) values (5, 'w2', 'c', 4); \
             delete from inventory where id = 3",
        )
        .await
        .expect("insert a new group's row and delete another group's only row");
    stage_cdc(
        &client,
        "inventory",
        "5",
        "insert",
        None,
        Some(r#"{"id":"5","warehouse":"w2","sku":"c","qty":"4"}"#),
    )
    .await;
    stage_cdc(
        &client,
        "inventory",
        "3",
        "delete",
        Some(r#"{"id":"3","warehouse":"w1","sku":"b","qty":"2"}"#),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        stock_totals(&client).await,
        HashMap::from([
            (("w1".to_string(), "a".to_string()), Some("12".to_string())),
            (("w2".to_string(), "a".to_string()), Some("11".to_string())),
            (("w2".to_string(), "c".to_string()), Some("4".to_string())),
        ]),
        "(w1, b) is extinct and (w2, c) is new"
    );
    assert_eq!(
        stock_totals_v2(&client).await,
        HashMap::from([
            // Issue #180: extinct (w1, b)'s 2 is now subtracted from `w1`
            // (14 - 2 = 12) instead of surviving forever.
            ("w1".to_string(), Some("12".to_string())),
            // The new `(w2, c)` group did propagate: 11 + 4.
            ("w2".to_string(), Some("15".to_string())),
        ]),
        "a brand-new composite-key group must propagate downstream (issue \
         #171) and an extinct one must now reduce the downstream aggregate \
         too (issue #180)"
    );
}

/// A grain migration on the *upstream* composite key — row 1 moves from
/// `(w1, a)` to `(w1, b)`, changing one of the two `GROUP BY` columns — must
/// propagate both the old and the new group key downstream. Both keys are
/// encoded by the same `derive_group_key` call pair
/// (`old_key`/`new_key`), so both go through issue #171's encoding on the way
/// out; `stock_totals_v2`'s own key (`warehouse`) is unchanged by this move,
/// which is precisely why its total must still be recomputed correctly from
/// two separate upstream keys rather than one.
#[tokio::test]
async fn a_live_grain_migration_of_the_composite_key_propagates_both_keys_downstream() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    install_the_chain(&db, &mut client).await;

    client
        .execute("update inventory set sku = 'b' where id = 1", &[])
        .await
        .expect("move inventory row 1 to another sku");
    stage_cdc(
        &client,
        "inventory",
        "1",
        "update",
        Some(r#"{"id":"1","warehouse":"w1","sku":"a","qty":"5"}"#),
        Some(r#"{"id":"1","warehouse":"w1","sku":"b","qty":"5"}"#),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        stock_totals(&client).await,
        HashMap::from([
            (("w1".to_string(), "a".to_string()), Some("7".to_string())),
            (("w1".to_string(), "b".to_string()), Some("7".to_string())),
            (("w2".to_string(), "a".to_string()), Some("11".to_string())),
        ]),
        "row 1's 5 units moved from (w1, a) to (w1, b)"
    );
    assert_eq!(
        stock_totals_v2(&client).await,
        HashMap::from([
            ("w1".to_string(), Some("14".to_string())),
            ("w2".to_string(), Some("11".to_string())),
        ]),
        "w1's downstream total is unchanged by an intra-warehouse move, but \
         only if both migrated composite keys decoded correctly"
    );
}
