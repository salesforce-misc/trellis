//! Integration tests for quarantine (issue #16, stage 06's other half):
//! isolate, evict, park, release, run against a real, ephemeral Postgres
//! instance via the shared harness (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/06-cleanup-and-reclaim.md for the design
//! these tests hold the implementation to. Every test builds its own source
//! table, definition, and target table by hand and stages changes directly
//! into the ring or the quarantine tables, matching `apply.rs`/`converge.rs`'s
//! own test conventions ("reach past the mechanism, insert directly" for
//! whichever half of a scenario this module itself doesn't produce).

use std::collections::HashMap;
use std::time::SystemTime;

use engine::config::DEFAULT_SCHEMA;
use engine::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use engine::defs::{
    CatalogError, DdlError, create_aggregate_target_table, create_definition, create_target_table,
    parse, recompute, source_primary_key,
};
use engine::staging::apply::{self, ApplyError, MAX_HOP_GEN};
use engine::staging::converge;
use engine::staging::{FoldedChange, isolate_and_evict};
use testkit::{TestCluster, TestDatabase};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};

/// Connects directly to `dsn` (bypassing `engine::Pool`), matching
/// `apply.rs`/`converge.rs`'s convention.
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
    use engine::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn segment_state_is_drained(client: &Client, seg_seq: i64) -> bool {
    let state: String = client
        .query_one("select state from segments where seg_seq = $1", &[&seg_seq])
        .await
        .expect("read segment state")
        .get(0);
    state == "drained"
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// Stages one image-bearing (CDC-shaped) change directly into `table`, with
/// an explicit `hop_gen` — `apply.rs`'s own `insert_cdc_row` always hardcodes
/// `hop_gen = 0`, but the halting-schema-error test here needs to seed a
/// change already at [`MAX_HOP_GEN`].
#[allow(clippy::too_many_arguments)]
async fn insert_cdc_row_with_hop_gen(
    client: &Client,
    table: &str,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
    hop_gen: i32,
) {
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, $7)"
            ),
            &[
                &src_table, &key, &op, &lsn, &old_image, &new_image, &hop_gen,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("insert cdc row {key:?} into {table} failed: {e}"));
}

async fn insert_cdc_row(
    client: &Client,
    table: &str,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    insert_cdc_row_with_hop_gen(client, table, src_table, key, op, old_image, new_image, 0).await;
}

fn order_totals_def() -> TransformDef {
    TransformDef {
        target: "order_totals".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::Column("tax".to_string())),
            },
        }],
        predicate: Predicate::True,
    }
}

/// Creates `orders` (unpopulated) plus the `order_totals` 1-1 definition and
/// target table — the shared fixture every test below builds on.
async fn seed_order_totals(db: &TestDatabase, client: &Client) -> TransformDef {
    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let def = order_totals_def();
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns)
        .await
        .expect("create target table");
    def
}

async fn insert_poison_marker(client: &Client, src_table: &str, key: &str) {
    client
        .execute(
            "insert into poison (src_table, key, last_error) values ($1, $2, 'test')",
            &[&src_table, &key],
        )
        .await
        .expect("insert poison marker");
}

#[allow(clippy::too_many_arguments)]
async fn insert_poison_held(
    client: &Client,
    src_table: &str,
    key: &str,
    seg_seq: i64,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
    origin_lsn: Option<u64>,
) {
    client
        .execute(
            "insert into poison_held \
                 (src_table, key, seg_seq, op, lsn, old_image, new_image, origin_lsn, hop_gen) \
             values ($1, $2, $3, $4, $5, $6::text::jsonb, $7::text::jsonb, $8, 0)",
            &[
                &src_table,
                &key,
                &seg_seq,
                &op,
                &Some(PgLsn::from(1u64)),
                &old_image,
                &new_image,
                &origin_lsn.map(PgLsn::from),
            ],
        )
        .await
        .expect("insert poison_held row");
}

async fn key_deaths_count(client: &Client, src_table: &str, key: &str) -> Option<i32> {
    client
        .query_opt(
            "select deaths from key_deaths where src_table = $1 and key = $2",
            &[&src_table, &key],
        )
        .await
        .expect("read key_deaths")
        .map(|row| row.get(0))
}

async fn poison_marker_exists(client: &Client, src_table: &str, key: &str) -> bool {
    client
        .query_opt(
            "select 1 from poison where src_table = $1 and key = $2",
            &[&src_table, &key],
        )
        .await
        .expect("read poison")
        .is_some()
}

async fn drain(pool: &engine::Pool, seg_seq: i64, claimed_by: &str) -> apply::ApplyOutcome {
    apply::drain_once(pool, seg_seq, claimed_by, 1, "trellis_quarantine_test")
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something")
}

/// Scenario: a poisoned key's parked contribution keeps its original
/// `origin_lsn`, so [`converge::converged_through`] never reports converged
/// across it. Release moves the row back onto the active batch (still
/// pending, now as an ordinary ring row) rather than discarding it — only an
/// actual drain of the released row clears the predicate.
#[tokio::test]
async fn quarantine_release_preserves_origin_position_so_convergence_stays_blocked() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .execute(
            "insert into replication_progress (slot_name, confirmed_lsn) values ('slot1', $1)",
            &[&PgLsn::from(1000u64)],
        )
        .await
        .expect("seed replication_progress");

    seed_order_totals(&db, &client).await;
    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed orders row");

    insert_poison_marker(&client, "orders", "1").await;
    insert_poison_held(
        &client,
        "orders",
        "1",
        1,
        "insert",
        None,
        Some(r#"{"id":"1","price":"10.00","tax":"1.50"}"#),
        Some(10),
    )
    .await;

    let token = PgLsn::from(50);
    assert!(
        !converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "a parked poison_held row with origin_lsn <= token must gate convergence"
    );

    let replayed = engine::staging::release_key(&db.pool, "orders", "1")
        .await
        .expect("release_key");
    assert_eq!(replayed, 1);
    assert!(
        !converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "the released row is now an ordinary pending ring row; the predicate must still block"
    );

    let seg_seq = seal_active_segment(&mut client).await;
    drain(&db.pool, seg_seq, "worker").await;
    assert!(
        converge::converged_through(&client, token)
            .await
            .expect("converged_through"),
        "once the released row actually drains, nothing pending remains"
    );
}

/// Scenario: a batch with one key whose staged image is malformed (an
/// evaluator-class failure — isolate-eligible, not transient/halting/fence)
/// and one healthy batch-mate. Below the death threshold, isolation
/// attributes the failure to the bad key alone, charges it exactly one
/// death, leaves the innocent key's counter and the `poison` marker
/// untouched for both, and surfaces the original error rather than blaming
/// or evicting anything (nothing has crossed the threshold yet).
#[tokio::test]
async fn an_innocent_batch_mate_is_not_charged_and_the_error_surfaces_unattributed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    client
        .batch_execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50), (2, 20.00, 2.00)",
        )
        .await
        .expect("seed orders rows");

    // Key "1" carries a malformed numeric field — fails `eval::evaluate`
    // (`EvalError::InvalidNumber`), an isolate-eligible failure. Key "2" is
    // healthy.
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "insert",
        None,
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let result = apply::drain_once(&db.pool, seg_seq, "worker", 1, "trellis_quarantine_test").await;
    match result {
        Err(ApplyError::Eval(_)) => {}
        other => panic!("expected an Eval error to surface, got {other:?}"),
    }

    assert_eq!(
        key_deaths_count(&client, "orders", "1").await,
        Some(1),
        "the key that actually fails alone must be charged exactly once"
    );
    assert_eq!(
        key_deaths_count(&client, "orders", "2").await,
        None,
        "the innocent batch-mate must not be charged at all"
    );
    assert!(
        !poison_marker_exists(&client, "orders", "1").await,
        "one death is below the default threshold; nothing is evicted yet"
    );
    assert!(!poison_marker_exists(&client, "orders", "2").await);
}

/// Scenario: a clean drain clears a key's death counter, so a stale death
/// from an earlier transient blip doesn't silently accumulate toward a false
/// eviction.
#[tokio::test]
async fn a_clean_drain_clears_death_counters() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed orders row");

    // A death recorded by some earlier isolate attempt, reached past here
    // directly rather than re-deriving it through a whole failing batch.
    client
        .execute(
            "insert into key_deaths (src_table, key, deaths, last_error) \
             values ('orders', '1', 3, 'earlier isolate attempt')",
            &[],
        )
        .await
        .expect("seed a pre-existing death count");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    drain(&db.pool, seg_seq, "worker").await;

    assert_eq!(
        key_deaths_count(&client, "orders", "1").await,
        None,
        "a clean drain must clear the death counter for every key it applied"
    );
}

/// Scenario: once a key is poisoned, every batch that excludes it must park
/// its own folded contribution before marking drained — so a healthy later
/// change to that key survives being excluded, rather than vanishing when
/// the excluding batch retires. Release replays the parked contribution and
/// it applies for real.
#[tokio::test]
async fn a_healthy_later_change_to_a_poisoned_key_survives_via_its_own_parked_contribution() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    client
        .batch_execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50), (2, 99.00, 9.00)",
        )
        .await
        .expect("seed orders rows");

    // Key "2" is already poisoned from some earlier, unrelated failure.
    insert_poison_marker(&client, "orders", "2").await;

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    // A perfectly healthy change to the poisoned key — must not be lost.
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "insert",
        None,
        Some(r#"{"price":"99.00","tax":"9.00"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 1, "only the non-poisoned key writes");

    let target_rows: i64 = client
        .query_one("select count(*) from order_totals where id = 2", &[])
        .await
        .expect("count order_totals rows for id 2")
        .get(0);
    assert_eq!(
        target_rows, 0,
        "the poisoned key's contribution must not have applied"
    );

    let held: i64 = client
        .query_one(
            "select count(*) from poison_held where src_table = 'orders' and key = '2' and seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("count poison_held rows")
        .get(0);
    assert_eq!(
        held, 1,
        "the excluding batch must have parked its own contribution for key 2"
    );

    let replayed = engine::staging::release_key(&db.pool, "orders", "2")
        .await
        .expect("release_key");
    assert_eq!(replayed, 1);

    let seg_seq2 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg_seq2, "worker").await;

    let total: String = client
        .query_one("select total::text from order_totals where id = 2", &[])
        .await
        .expect("read order_totals for id 2")
        .get(0);
    assert_eq!(
        total, "108.00",
        "the parked healthy change must apply once released, unmodified"
    );
}

/// Scenario: a halting schema diagnosis (the hop bound) must never be
/// quarantined — it propagates loudly, and increments the halting-stop
/// metric, regardless of how few or many keys it touches.
#[tokio::test]
async fn a_halting_schema_error_is_never_quarantined_and_stops_the_instance() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let def = seed_order_totals(&db, &client).await;
    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed orders row");

    // A second definition reading order_totals, so it has a downstream
    // reader of its own — required for the hop-bound check to trip at all.
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = create_definition(
        &db.pool,
        "TRANSFORM order_summary FROM order_totals SELECT total + total AS grand_total",
        &order_totals_columns,
    )
    .await
    .expect("create order_summary definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(
        &db.pool,
        &summary_def.def,
        "public",
        &pk,
        &order_totals_columns,
    )
    .await
    .expect("create order_summary table");

    // Already at the hop bound: applying and propagating once more must trip
    // it.
    insert_cdc_row_with_hop_gen(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
        MAX_HOP_GEN,
    )
    .await;

    let before = engine::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats before");

    let seg_seq = seal_active_segment(&mut client).await;
    let result = apply::drain_once(&db.pool, seg_seq, "worker", 1, "trellis_quarantine_test").await;
    match result {
        Err(ApplyError::HopBoundExceeded { .. }) => {}
        other => panic!("expected HopBoundExceeded to propagate, got {other:?}"),
    }

    assert!(
        !poison_marker_exists(&client, "orders", "1").await,
        "a halting schema error must never quarantine the key that triggered it"
    );

    let after = engine::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats after");
    assert_eq!(after.stop_count, before.stop_count + 1);
    assert!(after.last_reason.is_some());

    let written: i64 = client
        .query_one("select count(*) from order_totals where id = 1", &[])
        .await
        .expect("count order_totals rows")
        .get(0);
    assert_eq!(
        written, 0,
        "the transaction that discovered the hop bound must have rolled back entirely"
    );
}

/// Scenario: a composite source primary key (`DdlError::CompositePrimaryKeyUnsupported`)
/// is exactly as structural as the hop bound — "this definition can never
/// work against this source's real schema," not "this row's data is bad" —
/// so it must halt too, not get isolated and eventually evicted key by key.
/// Two keys touch the unsupported source; both would reproduce the failure
/// alone (it's schema-shaped, not row-shaped), which is precisely why
/// isolating it would be wrong: neither gets charged a death, and the
/// original error surfaces unmodified.
#[tokio::test]
async fn a_composite_primary_key_source_is_never_quarantined_and_stops_the_instance() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_lines (order_id integer, line_no integer, price numeric, \
             primary key (order_id, line_no))",
        )
        .await
        .expect("create source table with a composite primary key");

    let source_columns = numeric_columns(&["order_id", "line_no", "price"]);
    create_definition(
        &db.pool,
        "TRANSFORM line_totals FROM order_lines SELECT price AS total",
        &source_columns,
    )
    .await
    .expect("create definition over the composite-pk source");

    insert_cdc_row(
        &client,
        "seg_0",
        "order_lines",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"1","line_no":"1","price":"10.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "order_lines",
        "2",
        "insert",
        None,
        Some(r#"{"order_id":"1","line_no":"2","price":"20.00"}"#),
    )
    .await;

    let before = engine::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats before");

    let seg_seq = seal_active_segment(&mut client).await;
    let result = apply::drain_once(&db.pool, seg_seq, "worker", 1, "trellis_quarantine_test").await;
    match result {
        Err(ApplyError::Ddl(DdlError::CompositePrimaryKeyUnsupported { .. })) => {}
        other => panic!("expected CompositePrimaryKeyUnsupported to propagate, got {other:?}"),
    }

    assert_eq!(
        key_deaths_count(&client, "order_lines", "1").await,
        None,
        "a structural schema failure must not charge any key that merely touched it"
    );
    assert_eq!(key_deaths_count(&client, "order_lines", "2").await, None);
    assert!(!poison_marker_exists(&client, "order_lines", "1").await);
    assert!(!poison_marker_exists(&client, "order_lines", "2").await);

    let after = engine::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats after");
    assert_eq!(after.stop_count, before.stop_count + 1);
    assert!(after.last_reason.is_some());
}

/// OID-binding failures describe a broken schema contract, never malformed
/// per-key data. They must stop processing before isolation can charge or
/// evict unrelated source keys.
#[test]
fn catalog_binding_failures_are_halting() {
    for error in [
        ApplyError::Catalog(CatalogError::BoundSourceRelationMissing { oid: 1 }),
        ApplyError::Catalog(CatalogError::BoundSourceColumnMissing {
            relation_oid: 1,
            attnum: 2,
            logical_name: "amount".to_string(),
        }),
        ApplyError::Catalog(CatalogError::BoundSourceColumnIncompatible {
            relation_oid: 1,
            attnum: 2,
            logical_name: "amount".to_string(),
            expected_type_oid: 1700,
            expected_type_modifier: -1,
            actual_type_oid: 25,
            actual_type_modifier: -1,
        }),
        ApplyError::Catalog(CatalogError::TargetRelationNotFound {
            target: "totals".to_string(),
        }),
        ApplyError::Catalog(CatalogError::TargetRelationMismatch {
            target: "totals".to_string(),
            expected_oid: 1,
            actual_oid: 2,
        }),
    ] {
        assert_eq!(
            engine::staging::classify(&error),
            engine::staging::FailureClass::Halting,
            "{error:?} must halt rather than isolate"
        );
    }
}

/// Scenario: `poison_held` is idempotent on `(src_table, key, seg_seq)` — a
/// retried park for the exact same batch and key must not duplicate or
/// overwrite the row already parked for it.
#[tokio::test]
async fn poison_held_is_idempotent_on_table_key_batch() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let insert = "insert into poison_held \
                   (src_table, key, seg_seq, op, old_image, new_image, hop_gen) \
               values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0) \
                   on conflict do nothing";

    client
        .execute(
            insert,
            &[
                &"orders",
                &"1",
                &1i64,
                &"insert",
                &None::<&str>,
                &Some(r#"{"price":"10.00"}"#),
            ],
        )
        .await
        .expect("first park attempt");
    client
        .execute(
            insert,
            &[
                &"orders",
                &"1",
                &1i64,
                &"update",
                &None::<&str>,
                &Some(r#"{"price":"999.00"}"#),
            ],
        )
        .await
        .expect("second, conflicting park attempt for the same triple");

    let rows = client
        .query(
            "select op, new_image::text from poison_held where src_table = 'orders' and key = '1' and seg_seq = 1",
            &[],
        )
        .await
        .expect("read poison_held");
    assert_eq!(rows.len(), 1, "the retried park must not duplicate the row");
    let op: String = rows[0].get(0);
    let new_image: String = rows[0].get(1);
    assert_eq!(
        op, "insert",
        "the first park's content must survive, not be overwritten"
    );
    assert!(new_image.contains("10.00"));
}

/// Scenario: the dropped-table purge is the only path allowed to write a
/// sealed batch's rows — it deletes a dropped source table's staged rows
/// from every ring table (sealed slots included) so an undrainable batch
/// stops wedging the ring below it, and lets the drain retry and succeed.
#[tokio::test]
async fn dropped_table_purge_lets_a_wedged_batch_drain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table widgets (id integer primary key, name text)")
        .await
        .expect("seed source table");

    insert_cdc_row(
        &client,
        "seg_0",
        "widgets",
        "1",
        "insert",
        None,
        Some(r#"{"id":"1","name":"gadget"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    client
        .batch_execute("drop table widgets")
        .await
        .expect("drop the source table out from under the staged row");

    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 0);
    assert_eq!(outcome.keys_deleted, 0);
    assert!(
        segment_state_is_drained(&client, seg_seq).await,
        "purging the dropped table's rows must let the wedged batch actually drain"
    );

    let remaining: i64 = client
        .query_one(
            "select count(*) from (
                 select src_table from seg_0
                 union all select src_table from seg_1
                 union all select src_table from seg_2
                 union all select src_table from seg_3
             ) rows where src_table = 'widgets'",
            &[],
        )
        .await
        .expect("count remaining widgets rows")
        .get(0);
    assert_eq!(
        remaining, 0,
        "the purge must have removed the dropped table's rows from every ring table"
    );
}

/// An OID-specific dropped-table purge must not clear poison state belonging
/// to a replacement relation that reused the dropped table's presentation
/// name.
#[tokio::test]
async fn purge_of_an_old_oid_preserves_a_same_named_replacement_quarantine_state() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table orders (id integer primary key)")
        .await
        .expect("create original orders");
    let old_oid: u32 = client
        .query_one("select 'orders'::regclass::oid", &[])
        .await
        .expect("read original OID")
        .get(0);
    client
        .batch_execute("drop table orders; create table orders (id integer primary key)")
        .await
        .expect("replace orders relation");
    let replacement_oid: u32 = client
        .query_one("select 'orders'::regclass::oid", &[])
        .await
        .expect("read replacement OID")
        .get(0);
    assert_ne!(old_oid, replacement_oid);

    for oid in [old_oid, replacement_oid] {
        client
            .execute(
                "insert into poison (src_table, source_relation_oid, key, last_error) \
                 values ('orders', $1::oid, '1', 'test')",
                &[&oid],
            )
            .await
            .expect("seed OID poison marker");
        client
            .execute(
                "insert into key_deaths (src_table, source_relation_oid, key, deaths, last_error) \
                 values ('orders', $1::oid, '1', 1, 'test')",
                &[&oid],
            )
            .await
            .expect("seed OID death counter");
    }

    engine::staging::purge_dropped_table(&db.pool, "orders", Some(old_oid))
        .await
        .expect("purge original OID only");

    let remaining_poison: i64 = client
        .query_one(
            "select count(*) from poison where source_relation_oid = $1::oid and key = '1'",
            &[&replacement_oid],
        )
        .await
        .expect("read replacement poison")
        .get(0);
    let remaining_deaths: i64 = client
        .query_one(
            "select count(*) from key_deaths where source_relation_oid = $1::oid and key = '1'",
            &[&replacement_oid],
        )
        .await
        .expect("read replacement death count")
        .get(0);
    assert_eq!(remaining_poison, 1);
    assert_eq!(remaining_deaths, 1);
}

/// Scenario: release replays `poison_held` rows in batch order (`seg_seq`)
/// then position order, so a key with more than one parked contribution
/// telescopes to the same final state the oracle would produce, not to
/// whichever row happened to be inserted first.
#[tokio::test]
async fn release_replays_in_batch_then_position_order_and_telescopes_to_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let def = seed_order_totals(&db, &client).await;
    // The live source's final state matches the *later* (higher seg_seq)
    // parked contribution below.
    client
        .execute(
            "insert into orders (id, price, tax) values (1, 30.00, 3.00)",
            &[],
        )
        .await
        .expect("seed orders row");

    insert_poison_marker(&client, "orders", "1").await;
    // Two excluding batches' own parked contributions for the same key,
    // inserted out of seg_seq order here to prove release doesn't just
    // replay insertion order.
    insert_poison_held(
        &client,
        "orders",
        "1",
        2,
        "update",
        None,
        Some(r#"{"price":"30.00","tax":"3.00"}"#),
        Some(20),
    )
    .await;
    insert_poison_held(
        &client,
        "orders",
        "1",
        1,
        "update",
        None,
        Some(r#"{"price":"10.00","tax":"1.00"}"#),
        Some(10),
    )
    .await;

    let replayed = engine::staging::release_key(&db.pool, "orders", "1")
        .await
        .expect("release_key");
    assert_eq!(replayed, 2);

    let seg_seq = seal_active_segment(&mut client).await;
    drain(&db.pool, seg_seq, "worker").await;

    let source_columns = numeric_columns(&["id", "price", "tax"]);
    let oracle = recompute(&db.pool, &def, "id", &source_columns)
        .await
        .expect("oracle recompute");
    let total: String = client
        .query_one("select total::text from order_totals where id = 1", &[])
        .await
        .expect("read order_totals")
        .get(0);
    assert_eq!(
        total,
        oracle["1"]["total"]
            .as_ref()
            .map(|n| n.to_string())
            .unwrap(),
        "the final applied total must match the oracle's, not the earlier batch's parked value"
    );
    assert_eq!(total, "33.00", "batch 2 (seg_seq 2) must win over batch 1");
}

/// Scenario: doc 06's isolate-before-blaming can find that **no** single key
/// reproduces the failure alone — the failure only exists for the
/// combination. Two keys land in the same, brand-new aggregate group (no
/// target row exists yet) guarded by a `total <= 8` check constraint on the
/// running sum: each one's own contribution alone stays under the ceiling,
/// but folded into one batch their combined delta trips it. Isolation must
/// not blame either one — the original error surfaces unmodified, with
/// neither key charged a death.
///
/// A pre-existing target row is deliberately avoided here: Postgres
/// validates a table's CHECK constraints against the tentative
/// `INSERT`-shaped tuple before it even knows whether `ON CONFLICT DO
/// UPDATE`'s arbiter will fire, so a single-key probe against a group that
/// already has a committed row would be checked against that *insert*
/// shape (starting from zero) rather than the real accumulated total —
/// which is a Postgres/SQL-model quirk, not the isolate-before-blaming
/// scenario this test means to cover. Keeping the group brand new makes the
/// insert-shaped tuple the real, correct value for a single-key probe.
#[tokio::test]
async fn a_batch_failure_that_only_reproduces_combined_surfaces_unblamed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items (id integer primary key, order_id integer, amount numeric)",
        )
        .await
        .expect("create source table");

    let source = "TRANSFORM order_summary FROM order_items GROUP BY order_id \
                   SELECT order_id AS order_id, SUM(amount) AS total";
    let source_columns = numeric_columns(&["id", "order_id", "amount"]);
    create_definition(&db.pool, source, &source_columns)
        .await
        .expect("create aggregate definition");
    let def = parse(source).expect("parse aggregate definition");
    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create aggregate target table");
    client
        .batch_execute(
            "alter table order_summary add constraint total_at_most_8 check (total <= 8)",
        )
        .await
        .expect("add the check constraint the combined delta below must trip");

    // Both rows land in a group (order_id 1) that has never appeared
    // before, so a single-key probe's insert-shaped tuple is the real
    // value, not a false zero-baseline shape.
    client
        .batch_execute(
            "insert into order_items (id, order_id, amount) values (1, 1, 5.00), (2, 1, 5.00)",
        )
        .await
        .expect("seed live order_items rows");

    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"1","amount":"5.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "order_items",
        "2",
        "insert",
        None,
        Some(r#"{"order_id":"1","amount":"5.00"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;

    let result = apply::drain_once(&db.pool, seg1, "worker", 1, "trellis_quarantine_test").await;
    match &result {
        Err(ApplyError::Db(db_err)) => {
            assert_eq!(
                db_err.code(),
                Some(&SqlState::CHECK_VIOLATION),
                "expected the combined-delta check violation, got {db_err:?}"
            );
        }
        other => panic!("expected the combined batch's check violation to surface, got {other:?}"),
    }

    assert_eq!(
        key_deaths_count(&client, "order_items", "1").await,
        None,
        "neither key reproduces the failure alone, so neither is charged"
    );
    assert_eq!(key_deaths_count(&client, "order_items", "2").await, None);
    assert!(!poison_marker_exists(&client, "order_items", "1").await);
    assert!(!poison_marker_exists(&client, "order_items", "2").await);

    let target_row_exists: bool = client
        .query_one(
            "select exists(select 1 from order_summary where order_id = 1)",
            &[],
        )
        .await
        .expect("check whether the target row exists")
        .get(0);
    assert!(
        !target_row_exists,
        "the failed batch must not have partially applied"
    );
}

/// Scenario: doc 06's actual state machine driven end to end by real,
/// repeated failures — not a pre-seeded shortcut. A malformed key fails
/// every real attempt to apply it until its death count crosses
/// [`engine::staging::DEFAULT_DEATH_THRESHOLD`], at which point isolation
/// evicts it, parks its own excluded contribution, and the batch's
/// survivor(s) drain for real.
#[tokio::test]
async fn repeated_real_failures_cross_the_eviction_threshold_and_the_batch_still_drains() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    client
        .batch_execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50), (2, 20.00, 2.00)",
        )
        .await
        .expect("seed orders rows");

    // Key "1" is malformed and fails every real attempt to apply it; key
    // "2" is healthy throughout.
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "insert",
        None,
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let mut real_failures = 0;
    let outcome = loop {
        match apply::drain_once(&db.pool, seg_seq, "worker", 1, "trellis_quarantine_test").await {
            Ok(Some(outcome)) => break outcome,
            Ok(None) => panic!("drain_once claimed nothing on a still-undrained segment"),
            Err(_) => {
                real_failures += 1;
                assert!(
                    real_failures <= 20,
                    "did not cross the eviction threshold within a reasonable number of real attempts"
                );
            }
        }
    };

    assert!(
        real_failures >= 1,
        "the eviction must be driven by at least one real, observed failure, not conjured"
    );
    assert_eq!(outcome.keys_written, 1, "only the survivor, key 2, writes");

    assert!(
        poison_marker_exists(&client, "orders", "1").await,
        "the key must have actually crossed the threshold and been evicted"
    );
    let deaths = key_deaths_count(&client, "orders", "1")
        .await
        .expect("an evicted key's death counter is never cleared by eviction itself");
    assert!(
        deaths >= 5,
        "the counter must have reached the default threshold, got {deaths}"
    );

    let held: i64 = client
        .query_one(
            "select count(*) from poison_held \
             where src_table = 'orders' and key = '1' and seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("count poison_held rows")
        .get(0);
    assert_eq!(
        held, 1,
        "the batch that finally evicted the key must have parked its own excluded contribution"
    );

    assert!(
        segment_state_is_drained(&client, seg_seq).await,
        "the survivor(s) must let the batch actually reach drained"
    );

    let written: i64 = client
        .query_one("select count(*) from order_totals where id = 2", &[])
        .await
        .expect("count order_totals rows for id 2")
        .get(0);
    assert_eq!(written, 1, "the survivor must have applied");
    let missing: i64 = client
        .query_one("select count(*) from order_totals where id = 1", &[])
        .await
        .expect("count order_totals rows for id 1")
        .get(0);
    assert_eq!(missing, 0, "the evicted key must never have applied");
}

/// Scenario: `threshold == 0` genuinely disables eviction, not merely raises
/// the bar — a key already well past the default threshold from earlier
/// attempts (reached past the mechanism directly, per this file's
/// convention) must still never be poisoned or parked when isolation runs
/// with the fuse disabled, and its death counter must be left untouched.
#[tokio::test]
async fn zero_threshold_disables_eviction_even_past_the_default_threshold() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed orders row");

    client
        .execute(
            "insert into key_deaths (src_table, key, deaths, last_error) \
             values ('orders', '1', 10, 'earlier isolate attempts')",
            &[],
        )
        .await
        .expect("seed a death count already past the default threshold");

    let folded = vec![FoldedChange {
        src_table: "orders".to_string(),
        source_relation_oid: None,
        key: "1".to_string(),
        new_image: Some(r#"{"price":"not-a-number","tax":"1.50"}"#.to_string()),
        old_image: None,
        src_changed: None,
        origin_lsn: None,
        lsn: None,
        hop_gen: 0,
        first_seen: SystemTime::now(),
        group_key: None,
        is_truncate: false,
    }];

    let result = isolate_and_evict(&db.pool, 1, "worker", "trellis_quarantine_test", &folded, 0)
        .await
        .expect("isolate_and_evict with threshold 0 must not error");
    assert!(
        result.is_none(),
        "threshold 0 must never evict, regardless of how many deaths a key already has"
    );

    assert!(
        !poison_marker_exists(&client, "orders", "1").await,
        "threshold 0 must never poison a key"
    );
    assert_eq!(
        key_deaths_count(&client, "orders", "1").await,
        Some(10),
        "threshold 0 short-circuits before touching key_deaths at all"
    );
}
