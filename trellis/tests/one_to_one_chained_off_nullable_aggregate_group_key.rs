//! Front-door integration test for issue #205: a [`KeySpace::OneToOne`]
//! definition chained directly off an aggregate target's own (nullable)
//! `GROUP BY` key.
//!
//! ```text
//! TRANSFORM sku_totals      FROM sales      GROUP BY sku SELECT sum(amount) AS total
//! TRANSFORM sku_totals_echo FROM sku_totals              SELECT total AS echo_total
//! ```
//!
//! `sku_totals`' own primary key is its `sku` grouping column — genuinely
//! nullable (`UNIQUE NULLS NOT DISTINCT`, issue #128), unlike a real `PRIMARY
//! KEY`. Every consumer of the shared key-contract text this crate threads a
//! changed row's identity through (`ddl::pk_key_sql_expr`/`join_pk_key`) must
//! route a nullable column's part through issue #110's `NULL_KEY_SENTINEL`/
//! escape treatment — and decode it back out again before treating it as a
//! real value. `staging::apply::apply_target`'s write path used to skip that
//! decode, binding the still-encoded staged key text straight in as
//! `sku_totals_echo`'s own literal primary-key value: for a `NULL`-keyed
//! `sku` group, that meant storing the literal sentinel character (`chr(1)`)
//! as a real, non-`NULL` `sku` value — a phantom row no from-scratch backfill
//! would ever produce (`defs::backfill::discover_pk_ranges`'s ordered
//! `(lo, hi]` PK-range walk structurally never surfaces a `NULL`-keyed source
//! row, and `sku_totals_echo`'s own primary key is a real `primary key`
//! column, which Postgres makes `NOT NULL` unconditionally, so no row can
//! *ever* represent that group there — the only backfill-consistent answer is
//! "no row").
//!
//! The fix decodes `write.pk_text`/`delete.pk_text` through
//! `ddl::split_pk_key` before binding/matching it as a literal PK value:
//! `None` (a genuine `NULL`-keyed group) is now recognized and the key is
//! simply skipped (no row can represent it, matching backfill), and a real,
//! non-`NULL` group value that happens to contain a literal U+0001 is
//! unescaped back to its single-character form instead of being stored
//! doubled.
//!
//! [`KeySpace::OneToOne`]: trellis::defs::ast::KeySpace::OneToOne

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
            "oneone_chained_off_null_group_test",
            1,
            "trellis_oneone_chained_off_null_group_test",
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

/// `sales` needs `REPLICA IDENTITY FULL` because it's an aggregate source
/// (`sku_totals`' own delta path needs the old row image to find which group
/// to decrement). `sku_totals_echo` is a plain (non-aggregate)
/// [`KeySpace::OneToOne`], so its own source (`sku_totals`) needs no replica
/// identity widening — `catalog::assert_replica_identity_supports_aggregate`
/// only gates a `GROUP BY` definition's own source.
///
/// Deliberately seeded with no `NULL`-`sku` row yet: the `NULL` group arrives
/// later, purely as a live incremental change, so it exercises
/// `staging::apply::apply_target`'s write path (issue #205's actual bug)
/// rather than `sku_totals_echo`'s own from-scratch backfill (which this
/// target-shape gap can't reach in the first place — see this file's own
/// doc comment).
///
/// [`KeySpace::OneToOne`]: trellis::defs::ast::KeySpace::OneToOne
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table sales ( \
                 id integer primary key, sku text, amount integer \
             ); \
             alter table sales replica identity full; \
             insert into sales (id, sku, amount) values \
               (1, 'a', 5), (2, 'a', 7), (3, 'b', 2)",
        )
        .await
        .expect("create + seed the aggregate-then-1-1 chain's schema");
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

/// `sku -> echo_total`, including a row whose `sku` is literally the
/// `ddl::NULL_KEY_SENTINEL` character (`chr(1)`) — the corrupted shape this
/// issue is about — so a test can assert its *absence* directly rather than
/// only asserting the correct rows are present.
async fn sku_totals_echo(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query("select sku, echo_total::text from sku_totals_echo", &[])
        .await
        .expect("read sku_totals_echo")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

/// Installs both definitions and drains until both targets hold their
/// from-scratch backfilled state (over `create_schema`'s `NULL`-free seed
/// data only).
async fn install_the_chain(db: &testkit::TestDatabase, client: &mut Client) {
    install_definition(&db.pool, SKU_TOTALS, &sales_columns(), "public")
        .await
        .expect("install the aggregate");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, client).await;

    install_definition(&db.pool, SKU_TOTALS_ECHO, &sku_totals_columns(), "public")
        .await
        .expect("install the 1-1 chained onto the aggregate");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, client).await;

    assert_eq!(
        sku_totals(client).await,
        HashMap::from([
            (Some("a".to_string()), Some("12".to_string())),
            (Some("b".to_string()), Some("2".to_string())),
        ]),
        "from-scratch backfill of the upstream aggregate"
    );
    assert_eq!(
        sku_totals_echo(client).await,
        HashMap::from([
            ("a".to_string(), Some("12".to_string())),
            ("b".to_string(), Some("2".to_string())),
        ]),
        "from-scratch backfill of the chained 1-1 target"
    );
}

/// Issue #205's exact repro: a brand-new `NULL`-keyed `sales` row creates a
/// `NULL`-keyed group in `sku_totals` (already correct, per issue #110's
/// fix — asserted here only as a sanity check), which must then propagate
/// into `sku_totals_echo` *without* ever materializing a row keyed by the
/// literal sentinel text.
///
/// Grows the `NULL` group with a second row (exercising the delta path, not
/// just from-scratch creation) and then extinguishes it entirely, checking
/// after every step that `sku_totals_echo` never gains a `chr(1)`-keyed row
/// and its row count never exceeds the two real (`a`, `b`) groups.
#[tokio::test]
async fn a_null_keyed_upstream_group_never_stores_the_sentinel_in_the_chained_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    install_the_chain(&db, &mut client).await;

    // Creation: the NULL-keyed group's first row arrives live.
    client
        .batch_execute("insert into sales (id, sku, amount) values (7, null, 6)")
        .await
        .expect("insert the NULL-keyed group's first row");
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
        sku_totals(&client).await.get(&None).cloned(),
        Some(Some("6".to_string())),
        "sanity check (issue #110, already fixed): the NULL-keyed group is \
         maintained correctly at the aggregate level"
    );

    let echo_rows = sku_totals_echo(&client).await;
    assert_eq!(
        echo_rows,
        HashMap::from([
            ("a".to_string(), Some("12".to_string())),
            ("b".to_string(), Some("2".to_string())),
        ]),
        "issue #205: the NULL-keyed group's creation must not add any row to \
         the chained 1-1 target — sku_totals_echo's own primary key is a \
         real NOT NULL column, so no row can ever represent this group there \
         (the same answer a from-scratch backfill would give); before the \
         fix this asserted a spurious third row keyed by the literal \
         sentinel character instead"
    );
    assert_eq!(
        echo_rows.len(),
        2,
        "no phantom row for the NULL group, however it's keyed"
    );

    // Growth: a second NULL-keyed row arrives, exercising the delta
    // (non-from-scratch) path through apply_target again.
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
        sku_totals(&client).await.get(&None).cloned(),
        Some(Some("10".to_string())),
        "the NULL-keyed group grows upstream"
    );
    assert_eq!(
        sku_totals_echo(&client).await,
        HashMap::from([
            ("a".to_string(), Some("12".to_string())),
            ("b".to_string(), Some("2".to_string())),
        ]),
        "issue #205: growing the NULL-keyed group must still not add any row \
         to the chained 1-1 target"
    );

    // Extinction: deleting both rows removes the group entirely upstream.
    client
        .batch_execute("delete from sales where id in (7, 8)")
        .await
        .expect("delete the NULL-keyed group's rows");
    stage_cdc(
        &client,
        "sales",
        "7",
        "delete",
        Some(r#"{"id":"7","sku":null,"amount":"6"}"#),
        None,
    )
    .await;
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
        sku_totals(&client).await.get(&None).cloned(),
        None,
        "the NULL-keyed group goes extinct upstream, as it should"
    );
    assert_eq!(
        sku_totals_echo(&client).await,
        HashMap::from([
            ("a".to_string(), Some("12".to_string())),
            ("b".to_string(), Some("2".to_string())),
        ]),
        "issue #205: the NULL group's extinction leaves sku_totals_echo \
         exactly as it started — it never had a row to remove"
    );
}

/// Review follow-up, mirroring
/// `defs_aggregate_chained_single_column_group_key.rs`'s own
/// `a_real_sentinel_valued_group_key_stays_its_own_group`: a *real*,
/// non-`NULL` `sku` value that happens to be a lone U+0001 (`chr(1)`) —
/// [`ddl::NULL_KEY_SENTINEL`] itself — is a genuinely different group from
/// the `NULL`-keyed one, and `ddl::encode_key_part` escapes it (doubles it)
/// precisely so the two never collide. Before issue #205's fix,
/// `apply_target` bound that *doubled* text straight in as
/// `sku_totals_echo`'s literal primary-key value — two U+0001 characters,
/// not the source group's actual one-character key — silently corrupting
/// this group's identity in the chained target even though it isn't the
/// `NULL` case at all. The fix's decode ([`ddl::decode_key_part`]) collapses
/// it back to the real single-character value, matching what a from-scratch
/// backfill of `sku_totals_echo` would store.
///
/// [`ddl::NULL_KEY_SENTINEL`]: trellis::defs::ddl::NULL_KEY_SENTINEL
#[tokio::test]
async fn a_real_sentinel_character_group_key_round_trips_undoubled_in_the_chained_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;
    install_the_chain(&db, &mut client).await;

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

    let sentinel_echo_total: Option<String> = client
        .query_opt(
            r"select echo_total::text from sku_totals_echo where sku = E'\x01'",
            &[],
        )
        .await
        .expect("read the U+0001-keyed row in the chained target")
        .and_then(|row| row.get(0));
    assert_eq!(
        sentinel_echo_total,
        Some("5".to_string()),
        "issue #205: the chained target's row for a real chr(1)-valued group \
         key must be keyed by that single character, not a doubled pair — \
         before the fix, apply_target stored the doubled (still-escaped) \
         text as the literal PK value instead of decoding it first"
    );

    let doubled_sentinel_count: i64 = client
        .query_one(
            r"select count(*) from sku_totals_echo where sku = E'\x01\x01'",
            &[],
        )
        .await
        .expect("count any row wrongly keyed by the doubled sentinel")
        .get(0);
    assert_eq!(
        doubled_sentinel_count, 0,
        "no row should ever be keyed by the doubled (still-escaped) sentinel \
         text"
    );
}
