//! Regression test for a panic found during review of issue #121 (composite
//! primary keys for 1-1 transforms): a plain (non-aggregate)
//! [`KeySpace::OneToOne`] chained directly onto a **composite** aggregate
//! `GROUP BY` key — one of whose columns is nullable
//! (`create_aggregate_target_table`'s `UNIQUE NULLS NOT DISTINCT` grouping
//! constraint, issue #128) — could panic during its own from-scratch
//! backfill, rather than silently skipping the `NULL`-keyed group the way
//! `one_to_one_chained_off_nullable_aggregate_group_key.rs` already
//! documents for the live-apply path.
//!
//! ```text
//! TRANSFORM stock_totals FROM inventory     GROUP BY warehouse, sku SELECT sum(qty) AS total_qty
//! TRANSFORM stock_echo   FROM stock_totals                          SELECT total_qty AS echo_qty
//! ```
//!
//! `defs::backfill::discover_pk_ranges` reads each boundary column as a bare
//! `col::text` and binds it into a `Vec<String>` via `Row::get::<_, String>`,
//! which panics on a genuine SQL `NULL` (that accessor isn't `Option`-aware).
//! The only thing that stood between a `NULL` component and that panic was
//! whichever row won the outer `order by ... desc nulls last limit 1`
//! tie-break — and a row whose *leading* key column is a decisive,
//! non-`NULL` maximum wins that tie-break outright, independent of whether a
//! *later* column happens to be `NULL` for that same row (Postgres's
//! row-value comparison short-circuits on the first column pair that
//! decides the ordering). Concretely: seed `inventory` so the `NULL`-`sku`
//! row's `warehouse` (`'z'`) sorts after every other row's `warehouse`
//! (`'a'`) — that row alone then wins `discover_pk_ranges`' first boundary
//! probe, and its `sku` component is `NULL`.
//!
//! Fixed by excluding any row with a `NULL` key component from
//! `discover_pk_ranges`' candidate window outright (`<col> is not null`,
//! for every key column) — a no-op for a genuine `PRIMARY KEY` (always
//! `NOT NULL`), and, for the nullable fallback case, it keeps such a row out
//! of both the paging window and the boundary itself. That matches this
//! build's own invariant that a `NULL`-keyed group can never be represented
//! in a 1-1 target (whose own primary key is unconditionally `NOT NULL`):
//! `stock_echo` backfills to hold every real group and nothing for the
//! `NULL`-`sku` one, the same answer `sku_totals_echo` already got via the
//! live-apply path in `one_to_one_chained_off_nullable_aggregate_group_key.rs`.
//!
//! [`KeySpace::OneToOne`]: trellis::defs::ast::KeySpace::OneToOne

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue, install_definition};
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

async fn drain_backfill_chunks(pool: &trellis::Pool, target_schema: &str) {
    const CLAIMED_BY: &str = "composite_nullable_group_key_backfill_test_worker";
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

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

/// A plain [`KeySpace::OneToOne`] chained onto a composite aggregate `GROUP
/// BY` key must not panic (and must correctly omit the `NULL`-keyed group)
/// when it backfills from scratch over a source that already has a group
/// keyed partly `NULL` — see this file's own doc comment for exactly which
/// row shape triggers the bug this pins.
///
/// [`KeySpace::OneToOne`]: trellis::defs::ast::KeySpace::OneToOne
#[tokio::test]
async fn backfill_across_a_composite_nullable_group_key_skips_the_null_group_without_panicking() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table inventory ( \
                 id integer primary key, warehouse text, sku text, qty integer \
             ); \
             alter table inventory replica identity full; \
             insert into inventory (id, warehouse, sku, qty) values \
               (1, 'a', 'x', 5), (2, 'a', 'y', 7), (3, 'z', null, 2)",
        )
        .await
        .expect(
            "create + seed inventory: the NULL-sku row's warehouse ('z') is \
             the lexicographic max, so it wins discover_pk_ranges' outer \
             tie-break on the leading column alone, independent of its own \
             NULL trailing column",
        );

    install_definition(
        &db.pool,
        "TRANSFORM stock_totals FROM inventory GROUP BY warehouse, sku SELECT sum(qty) AS total_qty",
        &columns(&[
            ("id", ValueType::Numeric),
            ("warehouse", ValueType::Text),
            ("sku", ValueType::Text),
            ("qty", ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install the aggregate");

    let null_group_count: i64 = client
        .query_one("select count(*) from stock_totals where sku is null", &[])
        .await
        .expect("count NULL-sku groups")
        .get(0);
    assert_eq!(
        null_group_count, 1,
        "the NULL-sku group must already exist before the chained 1-1 backfills"
    );

    // stock_echo's from-scratch backfill walks stock_totals' own composite
    // (warehouse, sku) key via defs::backfill::discover_pk_ranges /
    // execute_one_to_one_chunk — this used to panic on the NULL-sku group.
    install_definition(
        &db.pool,
        "TRANSFORM stock_echo FROM stock_totals SELECT total_qty AS echo_qty",
        &columns(&[
            ("warehouse", ValueType::Text),
            ("sku", ValueType::Text),
            ("total_qty", ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install the chained OneToOne");

    drain_backfill_chunks(&db.pool, "public").await;

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    assert!(
        !has_pending(&client).await.expect("has_pending"),
        "nothing should be left pending after backfill"
    );

    let echo_rows: HashMap<(String, String), Option<String>> = client
        .query("select warehouse, sku, echo_qty::text from stock_echo", &[])
        .await
        .expect("read stock_echo")
        .into_iter()
        .map(|r| ((r.get(0), r.get(1)), r.get(2)))
        .collect();
    assert_eq!(
        echo_rows,
        HashMap::from([
            (("a".to_string(), "x".to_string()), Some("5".to_string())),
            (("a".to_string(), "y".to_string()), Some("7".to_string())),
        ]),
        "the NULL-sku group must never appear in the chained target — \
         stock_echo's own primary key is a real NOT NULL column, so no row \
         can ever represent that group there"
    );
}
