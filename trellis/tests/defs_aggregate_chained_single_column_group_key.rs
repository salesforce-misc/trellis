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
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
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
    let lsn = testkit::wal_insert_lsn(client).await;
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
    trellis::intake::publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, client).await;

    client
        .batch_execute("alter table sku_totals replica identity full")
        .await
        .expect("widen sku_totals's replica identity");

    install_definition(&db.pool, SKU_TOTALS_V2, &sku_totals_columns(), "public")
        .await
        .expect("install the chained aggregate reading sku_totals");
    trellis::intake::publication::settle_registrations(&db.pool).await;
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

/// The `NULL`-keyed group, which issue #180's own writeup calls out by name
/// ("a `NULL` grouping component ... same underlying gap, same failure
/// family") — and which issue #180's fix deliberately did **not** close,
/// because the gap was upstream of it, in the shared key contract itself:
/// `derive_group_key` used to encode a `NULL` component as an empty part
/// (indistinguishable from `''`), and the chained definition's own live
/// re-read (`read_live_rows_batch`'s keyset join, a plain `t.col = u.c0`)
/// could never match a `NULL` column anyway. Issue #110 closes it: a `NULL`
/// component now encodes as `ddl::NULL_KEY_SENTINEL` (a control character
/// that no genuine value can encode to, since `ddl::encode_key_part` escapes
/// a real one by doubling it — see
/// `a_real_sentinel_valued_group_key_stays_its_own_group` below), and
/// `read_live_rows_batch` uses `is not distinct from`
/// for any key component a batch's keys carry a `NULL` for.
///
/// This test (formerly `a_null_keyed_group_never_reaches_the_chained_downstream_aggregate`,
/// issue #195's pin of the gap) now asserts the behaviour we actually want,
/// end to end: a `NULL`-keyed upstream group's **creation**, a later
/// **update** that keeps it `NULL`-keyed, and its eventual **extinction**
/// must all reach the chained downstream aggregate, exactly like any other
/// group.
#[tokio::test]
async fn a_null_keyed_group_reaches_the_chained_downstream_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    install_the_chain(&db, &mut client).await;

    // Creation: the NULL-keyed group's only row arrives.
    client
        .batch_execute("insert into sales (id, sku, amount) values (7, null, 6)")
        .await
        .expect("insert the NULL-keyed group's only row");
    stage_cdc(
        &client,
        "sales",
        "7",
        "insert",
        None,
        Some(r#"{"id":"7","sku":null,"amount":"6"}"#),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        null_group_total(&client, "sku_totals", "total").await,
        Some("6".to_string()),
        "the NULL-keyed group is maintained correctly at its own level"
    );
    assert_eq!(
        null_group_total(&client, "sku_totals_v2", "total2").await,
        Some("6".to_string()),
        "issue #110: the NULL-keyed group's creation now propagates into \
         the chained aggregate"
    );

    // Update: a second NULL-keyed row arrives, growing the (still-NULL-keyed)
    // group — exercises the delta path, not just from-scratch creation.
    client
        .batch_execute("insert into sales (id, sku, amount) values (8, null, 4)")
        .await
        .expect("insert a second NULL-keyed row");
    stage_cdc(
        &client,
        "sales",
        "8",
        "insert",
        None,
        Some(r#"{"id":"8","sku":null,"amount":"4"}"#),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        null_group_total(&client, "sku_totals", "total").await,
        Some("10".to_string()),
        "the NULL-keyed group grows upstream"
    );
    assert_eq!(
        null_group_total(&client, "sku_totals_v2", "total2").await,
        Some("10".to_string()),
        "issue #110: the NULL-keyed group's update also propagates downstream"
    );

    // Partial reduction: deleting one of the two rows leaves the group alive
    // (not yet extinct), still NULL-keyed.
    client
        .batch_execute("delete from sales where id = 7")
        .await
        .expect("delete the first NULL-keyed row, leaving the group alive");
    stage_cdc(
        &client,
        "sales",
        "7",
        "delete",
        Some(r#"{"id":"7","sku":null,"amount":"6"}"#),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        null_group_total(&client, "sku_totals", "total").await,
        Some("4".to_string()),
        "the NULL-keyed group survives, reduced"
    );
    assert_eq!(
        null_group_total(&client, "sku_totals_v2", "total2").await,
        Some("4".to_string()),
        "issue #110: the reduction (not yet an extinction) propagates too"
    );

    // Extinction: deleting the group's last row must remove it both upstream
    // and (issue #180's image-threading, exercised here for a NULL key
    // specifically) downstream.
    client
        .batch_execute("delete from sales where id = 8")
        .await
        .expect("delete the NULL-keyed group's last remaining row");
    stage_cdc(
        &client,
        "sales",
        "8",
        "delete",
        Some(r#"{"id":"8","sku":null,"amount":"4"}"#),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        null_group_total(&client, "sku_totals", "total").await,
        None,
        "the NULL-keyed group goes extinct upstream, as it should"
    );
    assert_eq!(
        null_group_total(&client, "sku_totals_v2", "total2").await,
        None,
        "issue #110 + #180: the NULL-keyed group's extinction propagates \
         downstream too, instead of leaving a stale total2 = 4 forever"
    );
}

/// The `sku IS NULL` group's aggregate value in `table`, or `None` when that
/// group has no row at all — `sku_totals`/`sku_totals_v2`'s own helpers read
/// `sku` into a `String`, which a `NULL` key cannot be.
async fn null_group_total(client: &Client, table: &str, column: &str) -> Option<String> {
    client
        .query_opt(
            &format!("select {column}::text from {table} where sku is null"),
            &[],
        )
        .await
        .expect("read the NULL-keyed group")
        .and_then(|row| row.get(0))
}

/// Review follow-up to issue #110: `ddl::NULL_KEY_SENTINEL` is U+0001, an
/// ordinary control character a `text` column can genuinely hold — so the
/// encoding has to *escape* a real one rather than merely assume it never
/// occurs. Without the escape, a group whose `sku` is a lone U+0001 encodes
/// to exactly the same key text as the `sku IS NULL` group, and the two are
/// folded into one: the upstream aggregate loses the U+0001 group entirely
/// and mis-attributes its `amount` to the NULL group, at both levels of the
/// chain. That is a silent, data-dependent corruption of data that worked
/// correctly *before* #110, which makes it strictly worse than the bug #110
/// fixes — hence this pin, alongside `ddl`'s own unit-level round-trip test.
#[tokio::test]
async fn a_real_sentinel_valued_group_key_stays_its_own_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    install_the_chain(&db, &mut client).await;

    client
        .batch_execute("insert into sales (id, sku, amount) values (7, null, 6)")
        .await
        .expect("insert the NULL-keyed row");
    stage_cdc(
        &client,
        "sales",
        "7",
        "insert",
        None,
        Some(r#"{"id":"7","sku":null,"amount":"6"}"#),
    )
    .await;
    client
        .batch_execute(r"insert into sales (id, sku, amount) values (9, E'\x01', 5)")
        .await
        .expect("insert the U+0001-sku row");
    stage_cdc(
        &client,
        "sales",
        "9",
        "insert",
        None,
        Some(r#"{"id":"9","sku":"\u0001","amount":"5"}"#),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    async fn sentinel_total(client: &Client, table: &str, column: &str) -> Option<String> {
        client
            .query_opt(
                &format!(r"select {column}::text from {table} where sku = E'\x01'"),
                &[],
            )
            .await
            .expect("read the U+0001-keyed group")
            .and_then(|row| row.get(0))
    }

    assert_eq!(
        null_group_total(&client, "sku_totals", "total").await,
        Some("6".to_string()),
        "the NULL group keeps only its own row's amount"
    );
    assert_eq!(
        sentinel_total(&client, "sku_totals", "total").await,
        Some("5".to_string()),
        "and the U+0001 group is a separate group, not folded into the NULL one"
    );
    assert_eq!(
        null_group_total(&client, "sku_totals_v2", "total2").await,
        Some("6".to_string()),
        "both distinctions survive the chained hop's key round-trip"
    );
    assert_eq!(
        sentinel_total(&client, "sku_totals_v2", "total2").await,
        Some("5".to_string()),
        "the U+0001 group propagates downstream as its own group too"
    );
}
