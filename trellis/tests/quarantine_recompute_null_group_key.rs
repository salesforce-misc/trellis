//! Front-door integration test for issue #211:
//! `staging::quarantine::recompute_column` panics on a NULL-keyed group when
//! its source is a nullable-group-key aggregate target.
//!
//! ```text
//! TRANSFORM sku_totals      FROM sales      GROUP BY sku WITHOUT SOURCE INDEX SELECT sum(amount) AS total
//! TRANSFORM sku_totals_echo FROM sku_totals              SELECT total AS echo_total
//! ```
//!
//! Same fixture shape as issue #205's own regression test
//! (`one_to_one_chained_off_nullable_aggregate_group_key.rs`): `sku_totals`'
//! own primary key is its `sku` grouping column, genuinely nullable (`UNIQUE
//! NULLS NOT DISTINCT`, issue #128), unlike a real `PRIMARY KEY`. #205 closed
//! one producer/consumer of that fact — `staging::apply::apply_target`
//! binding a still-encoded staged key straight in as a literal PK. This is a
//! *different* producer/consumer of the very same fact: `recompute_column`'s
//! own query,
//!
//! ```text
//! select {pk_ident}::text as pk_text from {source}
//! ```
//!
//! does a raw, unencoded `{pk_ident}::text` cast directly against the source
//! table's column — nothing here ever goes through
//! `ddl::pk_key_sql_expr`/`NULL_KEY_SENTINEL`'s escape treatment at all, so
//! there's no encoding to `split_pk_key`-decode the way #205's fix did.
//! Instead, for the NULL-keyed group's row this is simply a genuine SQL
//! `NULL`, and the old `let pk_text: String = db_row.get(0);` panics —
//! `tokio-postgres` refuses to convert a `NULL` into a `String`.
//!
//! The fix reads `pk_text` as `Option<String>`. A column resume is now a
//! field build (#625 F8b), whose chunks' `(lo, hi]` ranges never reach a
//! `NULL`-keyed group either, so its plan stages that group's key into the
//! ring and the drain re-derives its row: `sku_totals_echo` is keyed `UNIQUE
//! NULLS NOT DISTINCT` like `sku_totals`, so it holds the `NULL` group's row
//! (issue #1141), and the resumed column is restored there too.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, install_definition};
use trellis::staging::quarantine;
use trellis::staging::{has_pending, retire_drained_segments};

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

/// Seals and drains repeatedly until nothing is pending anywhere in the ring
/// — needed after installing `sku_totals_echo` so its own downstream
/// propagation settles before this test reaches into `column_status`.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::apply;
    use trellis::staging::seal;
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "quarantine_recompute_null_group_key_test",
            1,
            "trellis_quarantine_recompute_null_group_key_test",
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

const SKU_TOTALS: &str =
    "TRANSFORM sku_totals FROM sales GROUP BY sku WITHOUT SOURCE INDEX SELECT sum(amount) AS total";
const SKU_TOTALS_ECHO: &str =
    "TRANSFORM sku_totals_echo FROM sku_totals SELECT total AS echo_total";

/// `sales`, seeded with a NULL-`sku` row from the very start (unlike #205's
/// own test, which deliberately introduced the NULL group later as a live
/// delta to exercise `apply_target`'s write path): this test exercises
/// `recompute_column`'s own direct read of the *already-built* `sku_totals`
/// table, not any live-CDC path, so the NULL group just needs to already
/// exist there by the time `recompute_column` runs.
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table sales ( \
                 id integer primary key, sku text, amount integer \
             ); \
             insert into sales (id, sku, amount) values \
               (1, 'a', 5), (2, 'a', 7), (3, 'b', 2), (4, null, 6)",
        )
        .await
        .expect("create + seed sales, including a NULL-sku row");
}

/// `sku -> total`, including the `NULL`-keyed group.
async fn sku_totals(client: &Client) -> HashMap<Option<String>, Option<String>> {
    client
        .query("select sku, total::text from sku_totals", &[])
        .await
        .expect("read sku_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// `sku -> echo_total` in the chained 1-1 target, including the
/// `NULL`-keyed group.
async fn sku_totals_echo(client: &Client) -> HashMap<Option<String>, Option<String>> {
    client
        .query("select sku, echo_total::text from sku_totals_echo", &[])
        .await
        .expect("read sku_totals_echo")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Issue #211's exact repro: pausing (reached past the mechanism, the same
/// "insert directly into `column_status`" convention
/// `tests/column_quarantine.rs` uses throughout) and then resuming
/// `sku_totals_echo.echo_total` re-derives the column over every current row
/// of `sku_totals` (the source), the NULL-keyed group's included. It must
/// not panic, and must restore the column on every row, the NULL-keyed
/// one's too (issue #1141).
#[tokio::test]
async fn resuming_a_column_restores_a_null_keyed_upstream_group_without_panicking() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    install_definition(&db.pool, SKU_TOTALS, &sales_columns(), "public")
        .await
        .expect("install the aggregate");
    trellis::intake::harness::settle_builds(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    install_definition(&db.pool, SKU_TOTALS_ECHO, &sku_totals_columns(), "public")
        .await
        .expect("install the 1-1 chained onto the aggregate");
    trellis::intake::harness::settle_builds(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    // Sanity checks: the upstream aggregate holds the NULL group, and so
    // does the chained 1-1 target (issue #1141).
    assert_eq!(
        sku_totals(&client).await.get(&None).cloned(),
        Some(Some("6".to_string())),
        "sanity check: the NULL-keyed group is present in the upstream aggregate"
    );
    let echo_before = sku_totals_echo(&client).await;
    assert_eq!(
        echo_before,
        HashMap::from([
            (Some("a".to_string()), Some("12".to_string())),
            (Some("b".to_string()), Some("2".to_string())),
            (None, Some("6".to_string())),
        ]),
        "sanity check: the build writes the NULL group's row too"
    );

    // Pause `echo_total` directly, then resume it — this is issue #211's
    // exact panic site: `recompute_column`'s own query reads every row of
    // `sku_totals`, including the NULL-keyed group's.
    client
        .execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('sku_totals_echo', 'echo_total', 'synthetic pause for issue #211 test', \
             true)",
            &[],
        )
        .await
        .expect("seed column_status directly");

    // Anti-vacuity: deliberately corrupt every row's `echo_total` first, the
    // NULL-keyed one's included, so the assertion below can tell "the
    // resume recomputed every row" apart from "it skipped some" (which
    // would leave a -1 behind).
    client
        .execute("update sku_totals_echo set echo_total = -1", &[])
        .await
        .expect("corrupt every row");

    let resumed = quarantine::resume_column(&db.pool, "sku_totals_echo", "echo_total")
        .await
        .expect(
            "resume_column must not panic when recompute_column's source query surfaces a \
             NULL-keyed group's row",
        );
    assert_eq!(
        resumed,
        vec![("sku_totals_echo".to_string(), "echo_total".to_string())],
        "only the resumed column itself"
    );
    // #625 F8b: the resume registers the column's field build; run it, then
    // drain the NULL-keyed key its plan staged.
    trellis::staging::build::settle_builds(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let echo_after = sku_totals_echo(&client).await;
    assert_eq!(
        echo_after, echo_before,
        "the resume restores every row's value, the NULL-keyed group's included"
    );
}
