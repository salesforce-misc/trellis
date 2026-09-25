//! Issue #308: `run_pending_backfills` wedged on a catch-up marker parked
//! against an *aggregate target* table.
//!
//! An aggregate target (`create_aggregate_target_table`) keys its `GROUP BY`
//! columns with `UNIQUE NULLS NOT DISTINCT`, not a `PRIMARY KEY` (a NULL
//! grouping value has to be representable). `enumerate_and_append` used to
//! look only for an `indisprimary` index, so enumerating such a table failed
//! with `MissingKeyValue`, the marker was never deleted, and every later
//! `run_pending_backfills` pass stopped at the same error — permanently
//! wedging catch-up processing (and failing client setup, which `?`s it).
//!
//! Both tests reach the enumeration the same way: an aggregate `sku_totals`
//! is chained into by a second definition, a settled marker sits on
//! `sku_totals`, and `sku_totals` has changed since its coverage fence (a
//! direct write here, so the enumeration can't be skipped and its effect is
//! observable downstream). They differ in the downstream definition's shape
//! and how the marker got parked:
//!
//! - a chunked 1-1 transform, whose `complete_direct_backfill` parks the
//!   marker itself (the trigger reachable on main);
//! - a second aggregate, with the marker parked directly (the shape
//!   ALTER TRANSFORM / column-resume catch-up parking produce), which also
//!   pins that the NULL-keyed group's encoded key decodes correctly
//!   downstream.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue, install_definition};
use trellis::intake::publication;
use trellis::staging::{has_pending, retire_drained_segments};

const TEST_NAME: &str = "aggregate_target_catchup_marker_test";

async fn drain_backfill_chunks(pool: &trellis::Pool) {
    // ADR-0016 (#418): registration only records a definition; the backfill
    // discharge dispatches its chunks.
    trellis::intake::publication::discharge_registrations(pool)
        .await
        .expect("dispatch registered definitions' builds");
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, TEST_NAME, 1000)
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
                TEST_NAME,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, TEST_NAME)
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

/// Seals and drains until nothing is pending anywhere in the ring. A bounded
/// deterministic loop, not a wait-for-convergence poll.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::apply;
    use trellis::staging::seal;
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            TEST_NAME,
            1,
            "trellis_aggregate_target_catchup_marker_test",
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

/// Installs [`SKU_TOTALS`], settles its background build (#419), and
/// discharges the catch-up marker its build parks on `sales` when it goes
/// live (issue #430), so each test starts with no marker but the one it is
/// about.
async fn install_sku_totals(pool: &trellis::Pool, client: &mut Client) {
    install_definition(pool, SKU_TOTALS, &sales_columns(), "public")
        .await
        .expect("install the aggregate");
    publication::settle_registrations(pool).await;
    publication::run_pending_backfills(
        client,
        "wake",
        &trellis::staging::StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("discharge the aggregate's go-live catch-up");
    drain_to_quiescence(pool, client).await;
}

/// `sales`, including a NULL-`sku` row so `sku_totals` has a NULL-keyed group
/// — the case a raw `sku::text` key would silently lose.
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table sales ( \
                 id integer primary key, sku text, amount integer \
             ); \
             alter table sales replica identity full; \
             insert into sales (id, sku, amount) values \
               (1, 'a', 5), (2, 'a', 7), (3, 'b', 2), (4, null, 6)",
        )
        .await
        .expect("create + seed sales, including a NULL-sku row");
}

async fn pending_markers(client: &Client) -> Vec<String> {
    client
        .query("select table_name from pending_backfill order by 1", &[])
        .await
        .expect("read pending_backfill")
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}

/// `sku -> value` for `select sku, <col>::text from <table>`.
async fn by_sku(
    client: &Client,
    table: &str,
    col: &str,
) -> HashMap<Option<String>, Option<String>> {
    client
        .query(&format!("select sku, {col}::text from {table}"), &[])
        .await
        .unwrap_or_else(|e| panic!("read {table}: {e}"))
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

fn some(s: &str) -> Option<String> {
    Some(s.to_string())
}

/// The trigger reachable on main: a chunked 1-1 transform chained off an
/// aggregate parks a catch-up marker on the aggregate target when its build
/// completes (`complete_direct_backfill`). Discharging it must enumerate the
/// aggregate target by its `UNIQUE NULLS NOT DISTINCT` grouping key.
#[tokio::test]
async fn catchup_marker_on_an_aggregate_target_feeding_a_one_to_one_discharges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    install_sku_totals(&db.pool, &mut client).await;
    install_definition(
        &db.pool,
        "TRANSFORM sku_totals_echo FROM sku_totals SELECT total AS echo_total",
        &sku_totals_columns(),
        "public",
    )
    .await
    .expect("install the 1-1 chained onto the aggregate");
    drain_backfill_chunks(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        pending_markers(&client).await,
        vec!["public.sku_totals".to_string()],
        "sanity check: completing the chunked build parked a catch-up marker on the aggregate \
         target"
    );

    // Change the aggregate target after its coverage fence, so the discharge
    // really enumerates it (and settles the marker's fence).
    client
        .execute("update sku_totals set total = 100 where sku = 'a'", &[])
        .await
        .expect("update sku_totals");

    publication::run_pending_backfills(
        &mut client,
        "wake",
        &trellis::staging::StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("issue #308: discharging a marker on an aggregate target must not fail");
    assert!(
        pending_markers(&client).await.is_empty(),
        "the discharged marker is deleted, not left to wedge the next pass"
    );
    publication::run_pending_backfills(
        &mut client,
        "wake",
        &trellis::staging::StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("a later pass is a clean no-op");

    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        by_sku(&client, "sku_totals_echo", "echo_total").await,
        HashMap::from([(some("a"), some("100")), (some("b"), some("2"))]),
        "the enumeration's keys reach the 1-1 target (the change to 'a' is folded in), and the \
         NULL-keyed group still has no representable row there"
    );
}

/// The shape ALTER TRANSFORM / column-resume catch-up parking produce: a
/// marker on an aggregate target that feeds a *second aggregate*. The
/// NULL-keyed group must be enumerated under its NULL-encoded key and decode
/// back to the `sku is null` row downstream.
#[tokio::test]
async fn catchup_marker_on_an_aggregate_target_feeding_an_aggregate_discharges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    install_sku_totals(&db.pool, &mut client).await;
    client
        .batch_execute("alter table sku_totals replica identity full")
        .await
        .expect("an aggregate's source needs replica identity full");
    install_definition(
        &db.pool,
        "TRANSFORM sku_totals_v2 FROM sku_totals GROUP BY sku SELECT sum(total) AS total2",
        &sku_totals_columns(),
        "public",
    )
    .await
    .expect("install the aggregate chained onto the aggregate");
    drain_backfill_chunks(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        by_sku(&client, "sku_totals_v2", "total2").await,
        HashMap::from([
            (some("a"), some("12")),
            (some("b"), some("2")),
            (None, some("6")),
        ]),
        "sanity check: the chained aggregate built every group, NULL included"
    );

    // Park the marker as `park_backfill_catchup` does: unfenced, for the
    // discharge to fence when it reads it.
    client
        .execute(
            "insert into pending_backfill (table_name) values ('public.sku_totals') \
             on conflict (table_name) do nothing",
            &[],
        )
        .await
        .expect("park a catch-up marker on the aggregate target");
    // Change both a real-keyed and the NULL-keyed group after the park.
    client
        .batch_execute(
            "update sku_totals set total = 100 where sku = 'a'; \
             update sku_totals set total = 60 where sku is null;",
        )
        .await
        .expect("update sku_totals");

    publication::run_pending_backfills(
        &mut client,
        "wake",
        &trellis::staging::StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("issue #308: discharging a marker on an aggregate target must not fail");
    assert!(
        pending_markers(&client).await.is_empty(),
        "the discharged marker is deleted, not left to wedge the next pass"
    );

    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        by_sku(&client, "sku_totals_v2", "total2").await,
        HashMap::from([
            (some("a"), some("100")),
            (some("b"), some("2")),
            (None, some("60")),
        ]),
        "every enumerated key, the NULL-keyed group's included, re-derives its downstream group"
    );
}
