//! Front-door integration test for issue #196, the sibling gap #180
//! deliberately scoped out: a **1-1 (non-aggregate) target row**'s deletion
//! must reduce a chained downstream aggregate, exactly like an extinct
//! upstream aggregate group's deletion does (issue #180).
//!
//! ```text
//! TRANSFORM order_view      FROM orders      SELECT customer AS customer, amount AS amt
//! TRANSFORM customer_totals FROM order_view  GROUP BY customer SELECT sum(amt) AS total
//! ```
//!
//! `order_view` is a plain [`KeySpace::OneToOne`] mirror of `orders` (no
//! `GROUP BY` at all) — one target row per source row, keyed by `orders`'
//! own primary key. Before issue #196's fix, `apply_and_mark_drained_many`'s
//! step 3 (`apply_target`'s ordinary delete) staged a deleted `order_view`
//! row's downstream propagation as an image-less `StagedChange::Recompute`.
//! `customer_totals`' own `accumulate_changes` would then live-refetch that
//! key, find nothing (the row is genuinely gone), and silently drop the
//! change instead of subtracting the deleted row's contribution — the exact
//! same failure shape issue #180 fixed for an *aggregate* group's own
//! extinction, just one producer earlier in the chain.
//!
//! The fix: `apply_target`'s own delete statement now captures each deleted
//! row's pre-delete image (`RETURNING ...`, an explicit per-column
//! `jsonb_build_object('<col>', <col>::text, ...)::text` since issue #248 —
//! `to_jsonb(t.*)::text` before it — the same shape
//! `apply_aggregate::delete_group_row`'s issue #180 fix uses), threaded
//! through the same `ChangedKey` slot so step 4 stages a real image-bearing
//! delete for it.
//!
//! [`KeySpace::OneToOne`]: trellis::defs::ast::KeySpace::OneToOne

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue, install_definition};
use trellis::staging::apply;
use trellis::staging::{has_pending, retire_drained_segments};

/// Claims and executes every pending direct-build backfill chunk until none
/// remain — the test-harness stand-in for a running drain worker, since
/// `install_definition` no longer runs a plain (non-relationship) 1-1
/// definition's backfill in-call (docs/decisions/0007's amendment): it
/// returns as soon as the chunk work is enumerated and persisted,
/// `Backfilling` until a drain worker actually claims and finishes each
/// chunk. `order_view` (this file's 1-1 mirror) needs this; `customer_totals`
/// (an aggregate) still backfills synchronously in-call and needs no chunk
/// draining. Mirrors `defs_install_definition.rs`'s helper of the same name.
async fn drain_backfill_chunks(pool: &trellis::Pool, target_schema: &str) {
    const CLAIMED_BY: &str = "oneone_deleted_target_test_backfill_worker";
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
                target_schema,
                CLAIMED_BY,
                Duration::from_secs(5),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

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
            "oneone_deleted_target_test",
            1,
            "trellis_oneone_deleted_target_test",
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

fn orders_columns() -> HashMap<String, ValueType> {
    columns(&[
        ("id", ValueType::Numeric),
        ("customer", ValueType::Text),
        ("amount", ValueType::Numeric),
    ])
}

fn order_view_columns() -> HashMap<String, ValueType> {
    columns(&[("customer", ValueType::Text), ("amt", ValueType::Numeric)])
}

const ORDER_VIEW: &str =
    "TRANSFORM order_view FROM orders SELECT customer AS customer, amount AS amt";
const CUSTOMER_TOTALS: &str =
    "TRANSFORM customer_totals FROM order_view GROUP BY customer SELECT sum(amt) AS total";

/// `orders` needs `REPLICA IDENTITY FULL` because `order_view` is a plain
/// mirror of it and this harness stages hand-built CDC rows carrying old
/// images directly; `order_view` needs it once `customer_totals` chains onto
/// it (an aggregate source's replica identity must support recovering an old
/// row image — `catalog::assert_replica_identity_supports_aggregate`), and
/// can only be widened after `install_definition` has created it.
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table orders ( \
                 id integer primary key, customer text, amount integer \
             ); \
             alter table orders replica identity full; \
             insert into orders (id, customer, amount) values \
               (1, 'a', 5), (2, 'a', 7), (3, 'b', 2), (4, 'c', 11)",
        )
        .await
        .expect("create + seed the 1-1-then-aggregate chain's schema");
}

/// `customer -> total`.
async fn customer_totals(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query("select customer, total::text from customer_totals", &[])
        .await
        .expect("read customer_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// `id -> (customer, amt)`, to assert `order_view`'s own 1-1 mirror stays
/// correct alongside the chained aggregate.
async fn order_view_rows(client: &Client) -> HashMap<String, (String, Option<String>)> {
    client
        .query("select id::text, customer, amt::text from order_view", &[])
        .await
        .expect("read order_view")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

/// Installs both definitions and drains until both targets hold their
/// from-scratch backfilled state.
async fn install_the_chain(db: &testkit::TestDatabase, client: &mut Client) {
    install_definition(&db.pool, ORDER_VIEW, &orders_columns(), "public")
        .await
        .expect("install the 1-1 order_view mirror");
    drain_backfill_chunks(&db.pool, "public").await;
    drain_to_quiescence(&db.pool, client).await;

    client
        .batch_execute("alter table order_view replica identity full")
        .await
        .expect("widen order_view's replica identity");

    install_definition(&db.pool, CUSTOMER_TOTALS, &order_view_columns(), "public")
        .await
        .expect("install the aggregate chained onto order_view");
    drain_to_quiescence(&db.pool, client).await;

    assert_eq!(
        customer_totals(client).await,
        HashMap::from([
            ("a".to_string(), Some("12".to_string())),
            ("b".to_string(), Some("2".to_string())),
            ("c".to_string(), Some("11".to_string())),
        ]),
        "from-scratch backfill of the chained aggregate"
    );
}

/// Issue #196's exact repro: deleting one `orders` row deletes its mirrored
/// `order_view` row (a plain 1-1 target delete, no `GROUP BY` anywhere
/// upstream of it), and that deletion must propagate into `customer_totals`
/// — whether it only *reduces* a surviving group (customer `a`, which keeps
/// a second order) or makes the group fully *extinct* (customer `b`, whose
/// only order is the one deleted) — rather than leaving `customer_totals`
/// stale forever. An unrelated ordinary update (customer `c`) and a
/// brand-new group (customer `d`) ride along in the same batch, each
/// deliberately in its own group so neither one masks a broken delete path
/// by coincidentally forcing the touched group through a full recompute.
#[tokio::test]
async fn a_deleted_oneone_target_row_reduces_the_chained_downstream_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    install_the_chain(&db, &mut client).await;

    // Customer 'a' keeps order 1 (amt 5) and loses order 2 (amt 7): a
    // partial reduction, 12 -> 5. Customer 'b' loses its only order (3, amt
    // 2): a full extinction, 2 -> gone. Customer 'c' gets an ordinary
    // update (11 -> 14, order 4's amount grows by 3). Customer 'd' is a
    // brand-new order (5, amt 9).
    client
        .batch_execute(
            "update orders set amount = 14 where id = 4; \
             insert into orders (id, customer, amount) values (5, 'd', 9); \
             delete from orders where id = 2; \
             delete from orders where id = 3",
        )
        .await
        .expect("update a survivor, insert a new group, delete two orders in two groups");
    stage_cdc(
        &client,
        "orders",
        "4",
        "update",
        Some(r#"{"id":"4","customer":"c","amount":"11"}"#),
        Some(r#"{"id":"4","customer":"c","amount":"14"}"#),
    )
    .await;
    stage_cdc(
        &client,
        "orders",
        "5",
        "insert",
        None,
        Some(r#"{"id":"5","customer":"d","amount":"9"}"#),
    )
    .await;
    stage_cdc(
        &client,
        "orders",
        "2",
        "delete",
        Some(r#"{"id":"2","customer":"a","amount":"7"}"#),
        None,
    )
    .await;
    stage_cdc(
        &client,
        "orders",
        "3",
        "delete",
        Some(r#"{"id":"3","customer":"b","amount":"2"}"#),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        order_view_rows(&client).await,
        HashMap::from([
            ("1".to_string(), ("a".to_string(), Some("5".to_string()))),
            ("4".to_string(), ("c".to_string(), Some("14".to_string()))),
            ("5".to_string(), ("d".to_string(), Some("9".to_string()))),
        ]),
        "order_view's own 1-1 mirror: rows 2 and 3 are gone, row 4 updated, \
         row 5 new"
    );
    assert_eq!(
        customer_totals(&client).await,
        HashMap::from([
            ("a".to_string(), Some("5".to_string())),
            ("c".to_string(), Some("14".to_string())),
            ("d".to_string(), Some("9".to_string())),
        ]),
        "issue #196: customer 'a' must reduce to 5 (order 2's deletion \
         subtracted, not dropped), customer 'b' must go fully extinct (not \
         left stale at total = 2), while the ordinary update ('c') and the \
         brand-new group ('d') still propagate normally"
    );
}
