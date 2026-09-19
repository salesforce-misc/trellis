//! Front-door integration test for issue #211:
//! `staging::quarantine::recompute_column` panics on a NULL-keyed group when
//! its source is a nullable-group-key aggregate target.
//!
//! ```text
//! TRANSFORM sku_totals      FROM sales      GROUP BY sku SELECT sum(amount) AS total
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
//! The fix reads `pk_text` as `Option<String>` and drops a `None` row before
//! it ever reaches `rows_by_pk`/`order`: exactly the same "no representable
//! row" reasoning #205 used for `apply_target`'s write path — a
//! `KeySpace::OneToOne` target's own primary key is always a real `primary
//! key` column (`ddl::create_target_table`), which Postgres makes `NOT NULL`
//! unconditionally regardless of the source column's own nullability, so no
//! row can ever represent a NULL-keyed group there, and there is nothing to
//! recompute (or write back) for it — matching what a from-scratch backfill
//! of this same target already, structurally, does
//! (`defs::backfill::discover_pk_ranges` never surfaces a NULL-keyed source
//! row into any chunk's range in the first place).

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue, install_definition};
use trellis::staging::quarantine;
use trellis::staging::{has_pending, retire_drained_segments};

/// Claims and executes every pending direct-build backfill chunk until none
/// remain — `sku_totals_echo` (a plain, non-aggregate `KeySpace::OneToOne`)
/// backfills via the durable chunk queue rather than in-call
/// (docs/decisions/0007's amendment), unlike `sku_totals` (an aggregate),
/// which still backfills synchronously inside `install_definition`. Mirrors
/// `one_to_one_chained_off_nullable_aggregate_group_key.rs`'s helper of the
/// same name.
async fn drain_backfill_chunks(pool: &trellis::Pool, target_schema: &str) {
    const CLAIMED_BY: &str = "quarantine_recompute_null_group_key_test_backfill_worker";
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
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
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
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq)
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "quarantine_recompute_null_group_key_test",
            1,
            "trellis_quarantine_recompute_null_group_key_test",
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
             alter table sales replica identity full; \
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

/// `sku -> echo_total` in the chained 1-1 target.
async fn sku_totals_echo(client: &Client) -> HashMap<String, Option<String>> {
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
/// `sku_totals_echo.echo_total` drives `resume_column` into
/// `recompute_column`, which reads every current row of `sku_totals` (the
/// source) — including the NULL-keyed group's row — via a raw
/// `sku::text` cast. Before the fix, `let pk_text: String = db_row.get(0)`
/// panicked on that row; the fix must instead recognize it has no
/// representable row in `sku_totals_echo` and simply skip it, exactly the
/// way #205 taught `apply_target`'s write path to.
#[tokio::test]
async fn recompute_column_skips_a_null_keyed_upstream_group_instead_of_panicking() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    install_definition(&db.pool, SKU_TOTALS, &sales_columns(), "public")
        .await
        .expect("install the aggregate (backfills synchronously, NULL group included)");
    drain_to_quiescence(&db.pool, &mut client).await;

    install_definition(&db.pool, SKU_TOTALS_ECHO, &sku_totals_columns(), "public")
        .await
        .expect("install the 1-1 chained onto the aggregate");
    drain_backfill_chunks(&db.pool, "public").await;
    drain_to_quiescence(&db.pool, &mut client).await;

    // Sanity checks, matching #205's already-fixed behavior: the upstream
    // aggregate holds the NULL group, but the chained 1-1 target has no row
    // for it (its own primary key column is real, NOT NULL).
    assert_eq!(
        sku_totals(&client).await.get(&None).cloned(),
        Some(Some("6".to_string())),
        "sanity check: the NULL-keyed group is present in the upstream aggregate"
    );
    let echo_before = sku_totals_echo(&client).await;
    assert_eq!(
        echo_before,
        HashMap::from([
            ("a".to_string(), Some("12".to_string())),
            ("b".to_string(), Some("2".to_string())),
        ]),
        "sanity check: from-scratch backfill never materializes a row for the NULL group"
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

    let echo_after = sku_totals_echo(&client).await;
    assert_eq!(
        echo_after, echo_before,
        "issue #211: recompute must not create a phantom row for the NULL-keyed group — \
         a_null-keyed source row has no representable row in this target, so the recompute \
         pass must simply skip it, the same as a from-scratch backfill already does"
    );
}
