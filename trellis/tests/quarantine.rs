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

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{
    DdlError, create_aggregate_target_table, create_definition, create_target_table,
    install_definition, parse, recompute, source_primary_key,
};
use trellis::staging::apply::{self, ApplyError, MAX_HOP_GEN};
use trellis::staging::converge;
use trellis::staging::{FoldedChange, StagedWatermark, isolate_and_evict};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `apply.rs`/`converge.rs`'s convention.
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

/// Bare (no `.`) `name` qualified under [`DEFAULT_SCHEMA`] — where every bare
/// `create table` in this file's own fixtures actually lands, since
/// `connect_raw` pins `search_path` to `{DEFAULT_SCHEMA}, public` and never
/// qualifies its own DDL. Matches `apply.rs`'s own `qualify_fixture_table`
/// (issue #74, ADR-0007): a CDC-staged `src_table`, and everything keyed off
/// it downstream (`poison`/`poison_held`/`key_deaths`, and `schema_nodes`'s
/// own lookups via `transforms_for_source`), must be fully qualified to
/// match what a real CDC producer stages (issue #76) and what the
/// dependency graph now keys on (issue #74). Already-qualified input
/// (containing a `.`) passes through unchanged.
fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
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
        explicit_source_schema: None,
        explicit_target_schema: None,
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
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
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

async fn drain(pool: &trellis::Pool, seg_seq: i64, claimed_by: &str) -> apply::ApplyOutcome {
    // Issue #132: a throwaway, always-caught-up watermark — no live
    // `Intake` runs in this test file, and it isn't exercising guard (a).
    apply::drain_once(
        pool,
        seg_seq,
        claimed_by,
        1,
        "trellis_quarantine_test",
        &StagedWatermark::saturated(),
    )
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

    let replayed = trellis::staging::release_key(&db.pool, "orders", "1")
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
    let orders = qualify_fixture_table("orders");
    insert_cdc_row(
        &client,
        "seg_0",
        &orders,
        "1",
        "insert",
        None,
        Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        &orders,
        "2",
        "insert",
        None,
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let result = apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    match result {
        Err(ApplyError::Eval(_)) => {}
        other => panic!("expected an Eval error to surface, got {other:?}"),
    }

    assert_eq!(
        key_deaths_count(&client, &orders, "1").await,
        Some(1),
        "the key that actually fails alone must be charged exactly once"
    );
    assert_eq!(
        key_deaths_count(&client, &orders, "2").await,
        None,
        "the innocent batch-mate must not be charged at all"
    );
    assert!(
        !poison_marker_exists(&client, &orders, "1").await,
        "one death is below the default threshold; nothing is evicted yet"
    );
    assert!(!poison_marker_exists(&client, &orders, "2").await);
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
    //
    // Seeded under the *qualified* spelling while the ring row below stays
    // bare — issue #283: `key_deaths` is keyed on the canonical identity of a
    // source table, so `clear_key_deaths` resolves the drained batch's raw
    // `src_table` before clearing. That makes this the sharper version of this
    // scenario: the counter is found and cleared across a spelling difference,
    // where before the fix the two spellings were two unrelated counters and a
    // bare-staged drain could only ever clear a bare-keyed row.
    let orders = qualify_fixture_table("orders");
    client
        .execute(
            "insert into key_deaths (src_table, key, deaths, last_error) \
             values ($1, '1', 3, 'earlier isolate attempt')",
            &[&orders],
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
        key_deaths_count(&client, &orders, "1").await,
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

    let orders = qualify_fixture_table("orders");
    // Key "2" is already poisoned from some earlier, unrelated failure.
    insert_poison_marker(&client, &orders, "2").await;

    insert_cdc_row(
        &client,
        "seg_0",
        &orders,
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
        &orders,
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
            "select count(*) from poison_held where src_table = $1 and key = '2' and seg_seq = $2",
            &[&orders, &seg_seq],
        )
        .await
        .expect("count poison_held rows")
        .get(0);
    assert_eq!(
        held, 1,
        "the excluding batch must have parked its own contribution for key 2"
    );

    let replayed = trellis::staging::release_key(&db.pool, &orders, "2")
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
        &summary_def.def.source,
    )
    .await
    .expect("create order_summary table");

    // Already at the hop bound: applying and propagating once more must trip
    // it.
    let orders = qualify_fixture_table("orders");
    insert_cdc_row_with_hop_gen(
        &client,
        "seg_0",
        &orders,
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
        MAX_HOP_GEN,
    )
    .await;

    let before = trellis::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats before");

    let seg_seq = seal_active_segment(&mut client).await;
    let result = apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    match result {
        Err(ApplyError::HopBoundExceeded { .. }) => {}
        other => panic!("expected HopBoundExceeded to propagate, got {other:?}"),
    }

    assert!(
        !poison_marker_exists(&client, &orders, "1").await,
        "a halting schema error must never quarantine the key that triggered it"
    );

    let after = trellis::staging::halting_stop_stats(&db.pool)
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

/// Scenario: a composite source primary key used to be exactly as structural
/// a rejection as the hop bound below (`DdlError::CompositePrimaryKeyUnsupported`,
/// pre-issue-#121) — "this definition can never work against this source's
/// real schema," not "this row's data is bad." Issue #121 lifted that
/// rejection: a `OneToOne` transform against a composite-PK source is now
/// accepted at create time and drains through the ring exactly like any
/// other 1-1 definition, so this test's own scenario no longer has anything
/// to reject — it instead pins the positive replacement: `create_definition`
/// against a composite-PK source succeeds, a live CDC insert drains cleanly
/// through the same `staging::apply` path this quarantine-focused test file
/// exercises for every other scenario, and neither `halting_stop_stats` nor
/// quarantine bookkeeping ever engages, because there is no failure left to
/// isolate or halt on. `defs_catalog.rs`'s
/// `a_one_to_one_transform_against_a_composite_primary_key_source_is_accepted`
/// and `one_to_one_composite_primary_key.rs`'s insert/update/delete coverage
/// are the more direct pins of this behavior; this test's own value
/// is narrower — confirming the old halt/quarantine path this file is about
/// genuinely never engages for this shape any more.
#[tokio::test]
async fn a_composite_primary_key_source_drains_cleanly_with_no_quarantine_or_halt() {
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
    // Seeded before `create_definition` so its own initial backfill
    // enumeration (`intake::publication::enumerate_and_append`) stages this
    // row as a `Recompute` into the ring, exercising the same
    // `staging::apply` drain path (pre-lock/upsert, no-op-suppression, key
    // decoding) every other scenario in this file drives.
    client
        .execute(
            "insert into order_lines (order_id, line_no, price) values (1, 1, 9.99)",
            &[],
        )
        .await
        .expect("seed a source row");

    let before = trellis::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats before");

    let source_columns = numeric_columns(&["order_id", "line_no", "price"]);
    let transform_text = "TRANSFORM line_totals FROM order_lines SELECT price AS total";
    create_definition(&db.pool, transform_text, &source_columns)
        .await
        .expect("issue #121: a composite-PK source is accepted, not rejected");
    // `create_definition` is the ring-path entry point (deliberately, not
    // `install_definition`) — it never runs target-table DDL itself, so the
    // caller builds the physical target here, matching `seed_order_totals`'s
    // own convention just above in this file.
    let def = parse(transform_text).expect("parse the transform");
    let pk = source_primary_key(&db.pool, "order_lines")
        .await
        .expect("introspect the composite source primary key");
    create_target_table(
        &db.pool,
        &def,
        "public",
        &pk,
        &source_columns,
        "order_lines",
    )
    .await
    .expect("create the composite-PK target table");

    let seg_seq = seal_active_segment(&mut client).await;
    drain(&db.pool, seg_seq, "worker").await;

    let total: String = client
        .query_one(
            "select total::text from line_totals where order_id = 1 and line_no = 1",
            &[],
        )
        .await
        .expect("the composite-PK target row was written")
        .get(0);
    assert_eq!(total, "9.99");

    assert_eq!(
        key_deaths_count(&client, "order_lines", "1\u{1f}1").await,
        None,
        "a clean drain must never charge any key a death"
    );
    assert!(!poison_marker_exists(&client, "order_lines", "1\u{1f}1").await);

    let after = trellis::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats after");
    assert_eq!(
        after.stop_count, before.stop_count,
        "a clean drain must not register as an instance halt"
    );
}

/// Scenario: a single-column primary key of an unsafe, non-text-stable type
/// (`DdlError::UnsupportedPrimaryKeyType`, issue #107) used to be exactly as
/// structural as the composite-key case above — "this definition can never
/// work against this source's real schema" — so it used to halt the
/// instance rather than get isolated and evicted key by key, for the exact
/// same reason the composite-key case did: `create_definition` never ran
/// `ddl::source_primary_key`/`require_single_column_pk` at all, so the
/// rejection only ever surfaced later, deep inside `staging::apply`.
///
/// Issue #177's fix closes this gap too, not just the composite-arity one
/// (which issue #121 later removed the rejection for — see this test's
/// sibling immediately above): `create_definition_inner`'s create-time key
/// check (every key-space since issue #371, see the aggregate sibling below)
/// calls `ddl::source_primary_key` itself (the same call
/// `install_definition` already made), and that function's own type check
/// (`is_text_stable_join_key_type`) runs unconditionally as part of fetching
/// the primary key — there's no way to ask it for "just the columns, skip
/// the type check" — so this scenario is still rejected up front. This test
/// pins the *absence* of the old halt, the same shape its sibling above
/// pins for the (now-accepted) composite-arity case.
///
/// `numeric` is the unsafe type here — `timestamptz` used to be this test's
/// example, but issue #246 (pinning `TimeZone` on the walsender the same way
/// `DateStyle` was already pinned on the pool) made it a text-stable primary
/// key type, so it no longer demonstrates this rejection.
#[tokio::test]
async fn an_unsupported_primary_key_type_source_is_rejected_at_create_time_not_quarantined_or_halted()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table events (occurred_at numeric primary key, payload text)")
        .await
        .expect("create source table with a numeric primary key");

    let before = trellis::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats before");

    let source_columns = numeric_columns(&["occurred_at", "payload"]);
    let err = create_definition(
        &db.pool,
        "TRANSFORM event_echo FROM events SELECT payload AS payload",
        &source_columns,
    )
    .await
    .unwrap_err();

    match &err {
        trellis::defs::CatalogError::Ddl(DdlError::UnsupportedPrimaryKeyType { .. }) => {}
        other => panic!(
            "expected create_definition to reject this up front with \
             UnsupportedPrimaryKeyType, got {other:?}"
        ),
    }

    assert_eq!(
        key_deaths_count(&client, "events", "1").await,
        None,
        "a create-time rejection must never charge any key a death"
    );
    assert!(!poison_marker_exists(&client, "events", "1").await);

    let after = trellis::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats after");
    assert_eq!(
        after.stop_count, before.stop_count,
        "a clean create-time rejection must not register as an instance halt"
    );
}

/// Issue #371: the aggregate sibling of the test above. Live apply reads
/// every source's primary key through `ddl::source_primary_key` whatever the
/// reading definition's key-space, and `quarantine::classify` halts the
/// instance on `UnsupportedPrimaryKeyType`. The create-time key check used to
/// run for 1-1 definitions only, so an aggregate over a non-text-stable key
/// installed cleanly and then halted the instance on its first live change.
/// Two shapes reached that halt:
///
/// - an aggregate over an ordinary table with a `numeric` primary key;
/// - an aggregate chained off another aggregate grouped by a `numeric`
///   column (that upstream target's identity is its `GROUP BY` key).
///
/// Both must now be refused up front, by both entry points
/// (`install_definition` and the ring-path `create_definition`), with nothing
/// left behind and no halt registered.
#[tokio::test]
async fn an_aggregate_over_an_unsupported_primary_key_type_source_is_rejected_at_create_time() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let before = trellis::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats before");

    let assert_unsupported_key =
        |what: &str, result: Result<_, trellis::defs::CatalogError>| match result {
            Err(trellis::defs::CatalogError::Ddl(DdlError::UnsupportedPrimaryKeyType {
                pg_type,
                ..
            })) => assert_eq!(pg_type, "numeric", "{what}"),
            Err(other) => panic!("{what}: expected UnsupportedPrimaryKeyType, got {other:?}"),
            Ok(_) => panic!("{what}: expected a create-time rejection, but it was accepted"),
        };

    // Shape 3: an aggregate over an ordinary table keyed on `numeric`.
    client
        .batch_execute(
            "create table ledger (entry_id numeric primary key, account text, cents integer); \
             alter table ledger replica identity full",
        )
        .await
        .expect("create source table with a numeric primary key");
    let ledger_columns: HashMap<String, ValueType> = [
        ("entry_id", ValueType::Numeric),
        ("account", ValueType::Text),
        ("cents", ValueType::Numeric),
    ]
    .into_iter()
    .map(|(n, t)| (n.to_string(), t))
    .collect();
    let account_totals = "TRANSFORM account_totals FROM ledger GROUP BY account \
                          SELECT account AS account, SUM(cents) AS total";
    assert_unsupported_key(
        "install_definition, aggregate over a numeric-keyed table",
        install_definition(&db.pool, account_totals, &ledger_columns, "public").await,
    );
    assert_unsupported_key(
        "create_definition, aggregate over a numeric-keyed table",
        create_definition(&db.pool, account_totals, &ledger_columns).await,
    );

    // Shape 2: an aggregate chained off a `numeric`-grouped aggregate. The
    // upstream itself is fine: its source is integer-keyed, and `numeric` is
    // an admitted `GROUP BY` key type.
    client
        .batch_execute(
            "create table sales (id integer primary key, price numeric, qty integer); \
             alter table sales replica identity full",
        )
        .await
        .expect("create integer-keyed source table");
    let sales_columns: HashMap<String, ValueType> = [
        ("id", ValueType::Numeric),
        ("price", ValueType::Numeric),
        ("qty", ValueType::Numeric),
    ]
    .into_iter()
    .map(|(n, t)| (n.to_string(), t))
    .collect();
    install_definition(
        &db.pool,
        "TRANSFORM price_totals FROM sales GROUP BY price \
         SELECT price AS price, SUM(qty) AS units",
        &sales_columns,
        "public",
    )
    .await
    .expect("a numeric-grouped aggregate over an integer-keyed table is accepted");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    client
        .batch_execute("alter table price_totals replica identity full")
        .await
        .expect("widen price_totals's replica identity");
    let price_totals_columns: HashMap<String, ValueType> =
        [("price", ValueType::Numeric), ("units", ValueType::Numeric)]
            .into_iter()
            .map(|(n, t)| (n.to_string(), t))
            .collect();
    let units_rollup = "TRANSFORM units_rollup FROM price_totals GROUP BY units \
                        SELECT units AS units, COUNT(*) AS prices";
    assert_unsupported_key(
        "install_definition, aggregate chained off a numeric-grouped aggregate",
        install_definition(&db.pool, units_rollup, &price_totals_columns, "public").await,
    );
    assert_unsupported_key(
        "create_definition, aggregate chained off a numeric-grouped aggregate",
        create_definition(&db.pool, units_rollup, &price_totals_columns).await,
    );

    // Neither rejected definition left a catalog row or a target table.
    for target in ["account_totals", "units_rollup"] {
        let rows: i64 = client
            .query_one(
                "select count(*) from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&target],
            )
            .await
            .expect("count catalog rows")
            .get(0);
        assert_eq!(rows, 0, "no catalog row for rejected {target}");
        let table: Option<String> = client
            .query_one(
                "select to_regclass($1)::text",
                &[&format!("public.{target}")],
            )
            .await
            .expect("probe target table")
            .get(0);
        assert_eq!(table, None, "no target table for rejected {target}");
    }

    let after = trellis::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats after");
    assert_eq!(
        after.stop_count, before.stop_count,
        "a create-time rejection must not register as an instance halt"
    );
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
                  on conflict (src_table, key, seg_seq) do nothing";

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

    let orders = qualify_fixture_table("orders");
    insert_poison_marker(&client, &orders, "1").await;
    // Two excluding batches' own parked contributions for the same key,
    // inserted out of seg_seq order here to prove release doesn't just
    // replay insertion order.
    insert_poison_held(
        &client,
        &orders,
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
        &orders,
        "1",
        1,
        "update",
        None,
        Some(r#"{"price":"10.00","tax":"1.00"}"#),
        Some(10),
    )
    .await;

    let replayed = trellis::staging::release_key(&db.pool, &orders, "1")
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
            "create table order_items (id integer primary key, order_id integer, amount numeric); \
             alter table order_items replica identity full",
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

    let order_items = qualify_fixture_table("order_items");
    insert_cdc_row(
        &client,
        "seg_0",
        &order_items,
        "1",
        "insert",
        None,
        Some(r#"{"order_id":"1","amount":"5.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        &order_items,
        "2",
        "insert",
        None,
        Some(r#"{"order_id":"1","amount":"5.00"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;

    let result = apply::drain_once(
        &db.pool,
        seg1,
        "worker",
        1,
        "trellis_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
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
        key_deaths_count(&client, &order_items, "1").await,
        None,
        "neither key reproduces the failure alone, so neither is charged"
    );
    assert_eq!(key_deaths_count(&client, &order_items, "2").await, None);
    assert!(!poison_marker_exists(&client, &order_items, "1").await);
    assert!(!poison_marker_exists(&client, &order_items, "2").await);

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
/// [`trellis::staging::DEFAULT_DEATH_THRESHOLD`], at which point isolation
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
    let orders = qualify_fixture_table("orders");
    insert_cdc_row(
        &client,
        "seg_0",
        &orders,
        "1",
        "insert",
        None,
        Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        &orders,
        "2",
        "insert",
        None,
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let mut real_failures = 0;
    let outcome = loop {
        match apply::drain_once(
            &db.pool,
            seg_seq,
            "worker",
            1,
            "trellis_quarantine_test",
            &StagedWatermark::saturated(),
        )
        .await
        {
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
        poison_marker_exists(&client, &orders, "1").await,
        "the key must have actually crossed the threshold and been evicted"
    );
    let deaths = key_deaths_count(&client, &orders, "1")
        .await
        .expect("an evicted key's death counter is never cleared by eviction itself");
    assert!(
        deaths >= 5,
        "the counter must have reached the default threshold, got {deaths}"
    );

    let held: i64 = client
        .query_one(
            "select count(*) from poison_held \
             where src_table = $1 and key = '1' and seg_seq = $2",
            &[&orders, &seg_seq],
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
        key: "1".to_string(),
        new_image: Some(r#"{"price":"not-a-number","tax":"1.50"}"#.to_string()),
        old_image: None,
        src_changed: None,
        origin_lsn: None,
        lsn: None,
        min_image_lsn: None,
        hop_gen: 0,
        first_seen: SystemTime::now(),
        group_key: None,
        is_truncate: false,
        relationship_reverse_deferred: None,
        retry_count: 0,
        prior_image: None,
        row_count: 1,
        has_recompute: false,
        vanished_images: Vec::new(),
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

/// Review follow-up to issue #134/#135: a `rel_reverse_deferred`
/// `FoldedChange` must never be probed, poisoned, or parked by
/// `isolate_and_evict` — its synthetic `src_table` (a per-relationship
/// sentinel, `staging::apply::relationship_reverse_deferred_src_table`) is
/// not a real table, `poison_held` has no columns for its
/// `relationship_id`/`retry_count` and no matching `op` value in its own
/// CHECK constraint, and a later `release_key` would re-append it as a
/// bogus `StagedChange::Cdc` against a table name that doesn't exist. This
/// pins the fix directly against `isolate_and_evict` (no ring/DB staging
/// needed at all: the skip happens before this function ever calls
/// `apply::compute`, so a synthetic key that doesn't correspond to
/// anything real is sufficient to prove it) — even given an *otherwise
/// maximally poison-prone* input (a `threshold` of 1, guaranteeing
/// eviction on the very first death for anything that *is* probed), the
/// deferred row must come out completely untouched: no probe, no death
/// charge, no poison marker, no parked contribution — and `isolate_and_evict`
/// itself must report nothing evicted, so its caller (`classify_and_retry`'s
/// `Isolate` arm) surfaces the original failure loudly instead of retrying
/// forever against an unresolvable "poisoned" synthetic key.
#[tokio::test]
async fn isolate_and_evict_never_probes_or_poisons_a_deferred_relationship_reverse() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // A synthetic sentinel `src_table` shaped exactly like
    // `relationship_reverse_deferred_src_table` produces — deliberately
    // not backed by any real table, relationship, or definition: if this
    // function ever tried to `compute()`/apply it, that alone would fail
    // loudly (a `SourceTableDropped`-shaped or catalog-lookup error), which
    // is precisely the point — this input has no legitimate way to
    // succeed except by being skipped outright.
    let sentinel_src_table = "\u{1f}trellis-rel-reverse-deferred:999";

    let folded = vec![FoldedChange {
        src_table: sentinel_src_table.to_string(),
        key: "1".to_string(),
        new_image: Some(r#"{"id":1,"v":2}"#.to_string()),
        old_image: Some(r#"{"id":1,"v":1}"#.to_string()),
        src_changed: None,
        origin_lsn: None,
        lsn: Some(PgLsn::from(1u64)),
        min_image_lsn: None,
        hop_gen: 0,
        first_seen: SystemTime::now(),
        group_key: None,
        is_truncate: false,
        relationship_reverse_deferred: Some(999),
        retry_count: 1,
        prior_image: None,
        row_count: 1,
        has_recompute: false,
        vanished_images: Vec::new(),
    }];

    let result = isolate_and_evict(&db.pool, 1, "worker", "trellis_quarantine_test", &folded, 1)
        .await
        .expect(
            "isolate_and_evict must not error on a deferred reverse — it must be skipped \
             outright, never probed",
        );
    assert!(
        result.is_none(),
        "a batch containing only a deferred reverse must report nothing evicted, so the \
         caller surfaces the original failure instead of silently 'resolving' it"
    );

    assert!(
        !poison_marker_exists(&client, sentinel_src_table, "1").await,
        "a deferred reverse must never be poisoned under its synthetic src_table"
    );
    assert_eq!(
        key_deaths_count(&client, sentinel_src_table, "1").await,
        None,
        "a deferred reverse must never be charged a death"
    );
    let poison_held_rows: i64 = client
        .query_one("select count(*) from poison_held", &[])
        .await
        .expect("count poison_held")
        .get(0);
    assert_eq!(
        poison_held_rows, 0,
        "a deferred reverse must never be parked into poison_held under its synthetic \
         src_table — release_key has no way to safely re-append it later"
    );
}

/// `target`'s persisted `transform_definitions.status`, matched on the bare
/// target-table suffix the same way `quarantine::resume_transform` does
/// (issue #73: the column itself is fully qualified).
async fn transform_status(client: &Client, target: &str) -> String {
    client
        .query_one(
            "select status from transform_definitions \
             where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read transform status")
        .get(0)
}

/// Marks `(src_table, key)` poisoned from inside `txn` — the marker half of
/// what `quarantine::evict_key` writes, staged directly (this file's
/// "reach past the mechanism, insert directly" convention) so the test below
/// controls exactly when each eviction commits.
async fn poison_in_txn(txn: &tokio_postgres::Transaction<'_>, src_table: &str, key: &str) {
    txn.execute(
        "insert into poison (src_table, key, last_error) values ($1, $2, 'test')",
        &[&src_table, &key],
    )
    .await
    .expect("insert poison marker inside a transaction");
}

/// How many backends on this cluster are currently parked waiting for a lock
/// — the signal the issue-#159 test below uses to observe worker B's fuse
/// check actually blocking on the fuse gate, rather than guessing with a
/// sleep. Read from a third, uninvolved connection.
async fn backends_waiting_on_a_lock(client: &Client) -> i64 {
    client
        .query_one(
            "select count(*) from pg_stat_activity where wait_event_type = 'Lock'",
            &[],
        )
        .await
        .expect("read pg_stat_activity")
        .get(0)
}

/// Regression pin for issue #159: two **concurrent** eviction transactions
/// for the same source table, neither of which crosses
/// `DEFAULT_TRANSFORM_DEATH_THRESHOLD` on the evidence it can see alone, must
/// still trip the whole-transform fuse once their evictions combine to cross
/// it.
///
/// The fuse counts `poison` rows inside the evicting transaction itself, so
/// it sees that transaction's own not-yet-committed insert — but, under READ
/// COMMITTED, *not* a sibling transaction's concurrent, still-uncommitted
/// one. With `threshold - 2` keys already evicted and two workers each
/// poisoning one more key (the source's 4th and 5th) before either commits,
/// both counts came back `threshold - 1`, neither tripped, and the transform
/// stayed live past its fuse point — indefinitely, unless some later,
/// unrelated eviction happened to re-run the check. `take_fuse_gate`'s
/// per-`src_table` row lock (`V30__transform_fuse_gate.sql`) is the fix: B
/// cannot begin counting until A's transaction has ended, so it counts all
/// five and trips.
///
/// **The interleaving is forced, not raced.** Both orderings this test cares
/// about are established by construction rather than by timing:
///
/// 1. A poisons its key and runs its fuse check to completion (four visible
///    keys — correctly declines to trip). Post-fix it now holds the gate.
/// 2. B's whole eviction transaction is started as a *concurrent* future and
///    driven under the same `tokio::join!` as step 3, so it is guaranteed to
///    have poisoned its own key and issued its count **before** A commits.
///    Post-fix that count parks on the gate; pre-fix it returns
///    `threshold - 1` immediately and B declines to trip, exactly the bug.
/// 3. A only commits once B is observably parked on a lock
///    (`backends_waiting_on_a_lock`) — or, pre-fix, once a bounded wait for
///    that has expired because B never blocked at all. Either way B's count
///    has already happened, so a pre-fix run cannot accidentally pass by
///    having B count after A's commit.
///
/// With the fix, step 3's commit releases the gate, B's count resumes and
/// sees all five, and the fuse trips. Without it, this test fails on the
/// final assertion every time.
///
/// Drives `trip_transform_fuse_if_crossed` directly with two hand-built
/// transactions rather than two `isolate_and_evict` calls: the race lives in
/// the window between one transaction's `poison` insert and its commit, which
/// two full `drain_once` pipelines cannot be made to overlap inside from the
/// outside.
#[tokio::test]
async fn concurrent_evictions_for_one_source_still_trip_the_whole_transform_fuse() {
    use std::time::Duration;

    use trellis::staging::quarantine::{
        DEFAULT_TRANSFORM_DEATH_THRESHOLD, trip_transform_fuse_if_crossed,
    };

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;
    let orders = qualify_fixture_table("orders");

    // Already committed: two short of the threshold, so neither transaction
    // below can cross it on the strength of its own single new eviction.
    for i in 0..(DEFAULT_TRANSFORM_DEATH_THRESHOLD - 2) {
        insert_poison_marker(&client, &orders, &format!("settled-{i}")).await;
    }
    assert_eq!(
        transform_status(&client, "order_totals").await,
        "live",
        "the fixture must start live — a quarantined definition is skipped by the fuse check, \
         and a non-live one is invisible to `transforms_for_source` in the first place"
    );

    let mut client_a = db.pool.get().await.expect("pool connection for worker a");
    let mut client_b = db.pool.get().await.expect("pool connection for worker b");

    // Worker A: poison one more key, check the fuse (sees `threshold - 1`,
    // correctly declines), and — post-fix — hold the gate until step 3.
    let txn_a = client_a.transaction().await.expect("begin worker a");
    poison_in_txn(&txn_a, &orders, "concurrent-a").await;
    trip_transform_fuse_if_crossed(&txn_a, &db.pool, &orders)
        .await
        .expect("worker a's fuse check");
    assert_eq!(
        transform_status(&client, "order_totals").await,
        "live",
        "worker a alone only reaches `threshold - 1` visible keys, so it must not trip the fuse"
    );

    let worker_b = async {
        let txn_b = client_b.transaction().await.expect("begin worker b");
        poison_in_txn(&txn_b, &orders, "concurrent-b").await;
        trip_transform_fuse_if_crossed(&txn_b, &db.pool, &orders)
            .await
            .expect("worker b's fuse check");
        txn_b.commit().await.expect("commit worker b");
    };
    let release_worker_a = async {
        // Bounded: with the fix, worker b parks on the gate within a round
        // trip or two and this exits at once; without it, worker b never
        // blocks and this simply expires, having already let b's (buggy,
        // `threshold - 1`) count happen first — which is the point.
        for _ in 0..200 {
            if backends_waiting_on_a_lock(&client).await > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        txn_a.commit().await.expect("commit worker a");
    };
    tokio::join!(worker_b, release_worker_a);

    let poisoned_keys: i64 = client
        .query_one(
            "select count(*) from poison where src_table = $1",
            &[&orders],
        )
        .await
        .expect("count poison")
        .get(0);
    assert_eq!(
        poisoned_keys, DEFAULT_TRANSFORM_DEATH_THRESHOLD as i64,
        "the two concurrent transactions must have committed a combined threshold's worth of \
         distinct evicted keys"
    );
    assert_eq!(
        transform_status(&client, "order_totals").await,
        "quarantined",
        "a threshold's worth of poisoned keys reached by two concurrent evictions must trip the \
         whole-transform fuse just as promptly as one worker reaching it alone (issue #159)"
    );
}

// ---------------------------------------------------------------------
// Issue #281: the whole-transform fuse must resolve a *bare* `src_table`
// before asking the catalog what to quarantine.
// ---------------------------------------------------------------------

/// A threshold's worth of `poison` rows must still trip the whole-transform
/// fuse when the fuse check itself is handed the **bare** spelling of their
/// source table (`orders`, not `public.orders`).
///
/// `trip_transform_fuse_if_crossed` used to hand its raw `src_table` straight
/// to `catalog::transforms_for_source`, whose contract (issue #74, ADR-0007)
/// requires an already-qualified name and whose non-qualified behaviour is to
/// return an *empty* definition set rather than an error. So for a bare
/// `src_table` the fuse counted its way past the threshold, logged nothing,
/// quarantined nothing, and returned `Ok(())` — the poisoning defence silently
/// absent for exactly the sources it was supposed to protect.
///
/// Issue #267 stopped `staging::apply` *emitting* a bare `src_table` going
/// forward, but bare names still reach this function from durable pre-#267 ring
/// rows, and from this crate's own integration fixtures (this file included)
/// that stage `src_table` by hand. Pre-fix this test's final assertion sees
/// `live`; post-fix the bare name is resolved through
/// `catalog::resolve_graph_identity` exactly as every `apply.rs` call site
/// already resolves it, and the fuse trips.
///
/// The `poison` rows themselves are staged **qualified** here, and were bare
/// when this test landed with #281: issue #283 made the qualified identity
/// `poison`'s canonical key rather than merely a spelling it might hold, so
/// staging them bare now models legacy on-disk state that
/// `V33__quarantine_canonical_src_table.sql` folds, not anything the live code
/// produces. What #281 is actually about — the *argument* reaching the fuse
/// bare, and having to be resolved before the catalog can answer with any
/// definitions at all — is unchanged and still exactly what this test drives.
/// The fold itself is covered by
/// `the_v33_fold_combines_dual_spelling_quarantine_rows`, and the
/// two-spellings-one-budget property by
/// `two_spellings_of_one_source_charge_one_combined_fuse_budget`.
#[tokio::test]
async fn a_bare_src_table_still_trips_the_whole_transform_fuse() {
    use trellis::staging::quarantine::{
        DEFAULT_TRANSFORM_DEATH_THRESHOLD, trip_transform_fuse_if_crossed,
    };

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;

    // Deliberately bare: no `qualify_fixture_table`, i.e. exactly what a
    // pre-#267 durable ring row (or a hand-staging fixture) leaves behind, and
    // what the fuse check below is handed.
    let bare = "orders";
    assert!(
        !bare.contains('.'),
        "the whole point of this test is an unqualified spelling"
    );
    let orders = qualify_fixture_table(bare);
    for i in 0..DEFAULT_TRANSFORM_DEATH_THRESHOLD {
        insert_poison_marker(&client, &orders, &format!("bare-{i}")).await;
    }
    assert_eq!(
        transform_status(&client, "order_totals").await,
        "live",
        "the fixture must start live — a non-live definition is invisible to \
         `transforms_for_source` in the first place"
    );

    let mut fuse_client = db.pool.get().await.expect("pool connection");
    let txn = fuse_client.transaction().await.expect("begin");
    trip_transform_fuse_if_crossed(&txn, &db.pool, bare)
        .await
        .expect("fuse check must not error on a bare src_table");
    txn.commit().await.expect("commit");

    assert_eq!(
        transform_status(&client, "order_totals").await,
        "quarantined",
        "a threshold's worth of poison rows under a bare `src_table` must quarantine the \
         transform on that source, not silently resolve to zero definitions (issue #281)"
    );
}

/// The `RelationshipReverseDeferred` sentinel `src_table`
/// (`apply::relationship_reverse_deferred_src_table` — U+001F-prefixed, and
/// neither bare nor qualified) is issue #281's one genuinely unresolvable
/// spelling: it names no physical table and no definition's target, so the
/// bare-name resolution the fix adds *cannot* succeed for it. It must fall
/// through to the pre-existing "no definitions, nothing to do" outcome rather
/// than surfacing `CatalogError::SourceTableNotFound` as a brand-new
/// `ApplyError` from inside an eviction transaction.
///
/// (`isolate_and_evict` already skips deferred-reverse rows before they can
/// reach the fuse at all — see
/// `isolate_and_evict_never_probes_or_poisons_a_deferred_relationship_reverse`
/// above — so this pins the belt-and-braces behaviour of the `pub` function
/// itself, which is also what a hand-written `poison` row for a since-dropped
/// source table would hit.)
#[tokio::test]
async fn an_unresolvable_src_table_leaves_the_fuse_a_quiet_no_op() {
    use trellis::staging::quarantine::{
        DEFAULT_TRANSFORM_DEATH_THRESHOLD, trip_transform_fuse_if_crossed,
    };

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;

    for unresolvable in [
        "\u{1f}trellis-rel-reverse-deferred:7",
        "long_since_dropped_table",
    ] {
        for i in 0..DEFAULT_TRANSFORM_DEATH_THRESHOLD {
            insert_poison_marker(&client, unresolvable, &format!("k-{i}")).await;
        }
        let mut fuse_client = db.pool.get().await.expect("pool connection");
        let txn = fuse_client.transaction().await.expect("begin");
        trip_transform_fuse_if_crossed(&txn, &db.pool, unresolvable)
            .await
            .unwrap_or_else(|e| {
                panic!("fuse check must not error on the unresolvable {unresolvable:?}: {e}")
            });
        txn.commit().await.expect("commit");
    }

    assert_eq!(
        transform_status(&client, "order_totals").await,
        "live",
        "an unresolvable `src_table` names no definition, so it must quarantine nothing — \
         least of all an unrelated live transform on a real source"
    );
}

// ---------------------------------------------------------------------
// Issue #283: quarantine's counter/marker tables key on one *canonical*
// identity per logical source table, not on the raw ring spelling.
// ---------------------------------------------------------------------

/// One `FoldedChange` for `src_table`/`key` whose image cannot evaluate
/// (`price` is not a number), i.e. a change guaranteed to reproduce an
/// `Isolate`-class failure when `isolate_and_evict` probes it alone — the
/// shape `zero_threshold_disables_eviction_even_past_the_default_threshold`
/// already uses to drive that function directly.
fn unevaluable_change(src_table: &str, key: &str) -> FoldedChange {
    FoldedChange {
        src_table: src_table.to_string(),
        key: key.to_string(),
        new_image: Some(r#"{"price":"not-a-number","tax":"1.50"}"#.to_string()),
        old_image: None,
        src_changed: None,
        origin_lsn: None,
        lsn: None,
        min_image_lsn: None,
        hop_gen: 0,
        first_seen: SystemTime::now(),
        group_key: None,
        is_truncate: false,
        relationship_reverse_deferred: None,
        retry_count: 0,
        prior_image: None,
        row_count: 1,
        has_recompute: false,
        vanished_images: Vec::new(),
    }
}

async fn poison_rows_for(client: &Client, src_table: &str) -> i64 {
    client
        .query_one(
            "select count(*) from poison where src_table = $1",
            &[&src_table],
        )
        .await
        .expect("count poison rows")
        .get(0)
}

/// The headline property of issue #283: a threshold's worth of evictions spread
/// across **two spellings of one logical source table** charges **one** combined
/// whole-transform fuse budget, and trips it.
///
/// Every quarantine counter/marker table used to store and match whatever
/// `src_table` spelling the ring row being diagnosed happened to carry. A source
/// staged both bare (`orders` — pre-#267 durable ring rows, hand-staging
/// fixtures) and qualified (`public.orders`) therefore ran two entirely
/// independent sets of quarantine state: here, five real evictions for five
/// distinct keys of one physical table split into a 3-row budget and a 2-row
/// budget, neither reaching `DEFAULT_TRANSFORM_DEATH_THRESHOLD`, so the fuse
/// never tripped even though the source had killed a full threshold's worth of
/// rows. Pre-fix this test's final assertion sees `live`.
///
/// Driven through `isolate_and_evict` itself rather than hand-inserted `poison`
/// rows, deliberately: the fix is that the *write* side resolves `src_table` to
/// its canonical identity before touching any of these tables, so a test that
/// staged the markers directly would be asserting its own spelling choice rather
/// than the mechanism's. `threshold = 1` evicts on each key's first observed
/// death, keeping the scenario to one eviction per call; the whole-transform
/// fuse's own threshold is untouched and is what the assertions below turn on.
#[tokio::test]
async fn two_spellings_of_one_source_charge_one_combined_fuse_budget() {
    use trellis::staging::quarantine::DEFAULT_TRANSFORM_DEATH_THRESHOLD;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;

    let qualified = qualify_fixture_table("orders");
    let bare = "orders";

    // Alternating spellings, one eviction per key: with a threshold of 5 that
    // is 3 bare and 2 qualified — pre-fix, two independent budgets of 3 and 2.
    for i in 0..DEFAULT_TRANSFORM_DEATH_THRESHOLD {
        let src_table = if i % 2 == 0 { bare } else { qualified.as_str() };
        let folded = vec![unevaluable_change(src_table, &format!("{i}"))];
        let retry = isolate_and_evict(&db.pool, 1, "worker", "trellis_quarantine_test", &folded, 1)
            .await
            .expect("isolate_and_evict must not error on an unevaluable change");
        let retry = retry.unwrap_or_else(|| {
            panic!("the key staged under {src_table:?} must have been evicted at threshold 1")
        });
        assert!(
            retry.is_empty(),
            "the evicted key was the batch's only change, so nothing is left to retry"
        );
    }

    assert_eq!(
        poison_rows_for(&client, bare).await,
        0,
        "no quarantine row may be written under the raw bare spelling any more — the canonical \
         identity is the key (issue #283)"
    );
    assert_eq!(
        poison_rows_for(&client, &qualified).await,
        DEFAULT_TRANSFORM_DEATH_THRESHOLD as i64,
        "all five evictions must land in one budget under the canonical identity, however the \
         ring row that produced each of them was spelled"
    );
    assert_eq!(
        transform_status(&client, "order_totals").await,
        "quarantined",
        "a threshold's worth of evictions for one logical source must trip the whole-transform \
         fuse even when they arrive under two different spellings of it (issue #283)"
    );
}

/// The same canonical keying, one tier down: two spellings of one source must
/// charge **one** row-level death counter for the same physical row, not two
/// independent ones (which made the row-level fuse take up to twice as many real
/// failures to fire).
#[tokio::test]
async fn two_spellings_of_one_row_charge_one_death_counter() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;

    let qualified = qualify_fixture_table("orders");

    // Threshold 3, and three real failures for one key — arriving under the
    // bare spelling twice and the qualified spelling once. Pre-fix that is a
    // counter of 2 and a counter of 1, and nothing is ever evicted.
    for src_table in ["orders", qualified.as_str(), "orders"] {
        let folded = vec![unevaluable_change(src_table, "1")];
        isolate_and_evict(&db.pool, 1, "worker", "trellis_quarantine_test", &folded, 3)
            .await
            .expect("isolate_and_evict must not error on an unevaluable change");
    }

    assert_eq!(
        key_deaths_count(&client, "orders", "1").await,
        None,
        "nothing may be counted under the raw bare spelling (issue #283)"
    );
    assert_eq!(
        key_deaths_count(&client, &qualified, "1").await,
        Some(3),
        "all three observed deaths for one physical row must land on one counter"
    );
    assert!(
        poison_marker_exists(&client, &qualified, "1").await,
        "the combined counter must reach the threshold and evict the key — pre-fix the two split \
         counters reached 2 and 1 and it never did (issue #283)"
    );
}

/// Issue #159's serialization row lock (`transform_fuse_gate`) must serialize
/// two concurrent evictions for one logical source **across spellings** — it was
/// keyed per spelling, so two workers evicting the same source under different
/// names took two different lock rows and serialized against nothing, which is
/// the exact race that table exists to close.
///
/// Same forced interleaving as
/// `concurrent_evictions_for_one_source_still_trip_the_whole_transform_fuse`
/// (see its doc comment for why each ordering holds by construction rather than
/// by timing), with one difference: worker A checks the fuse under the bare
/// spelling and worker B under the qualified one. Pre-fix neither blocks on the
/// other and neither trips — A counts zero rows under its bare name, B counts
/// `threshold - 1` under its own — so the final assertion sees `live`. Post-fix
/// both resolve to one gate row, B cannot count until A commits, and B's count
/// includes A's row and trips.
#[tokio::test]
async fn the_fuse_gate_serializes_two_spellings_of_one_source() {
    use std::time::Duration;

    use trellis::staging::quarantine::{
        DEFAULT_TRANSFORM_DEATH_THRESHOLD, trip_transform_fuse_if_crossed,
    };

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;
    let qualified = qualify_fixture_table("orders");
    let bare = "orders";

    for i in 0..(DEFAULT_TRANSFORM_DEATH_THRESHOLD - 2) {
        insert_poison_marker(&client, &qualified, &format!("settled-{i}")).await;
    }
    assert_eq!(
        transform_status(&client, "order_totals").await,
        "live",
        "the fixture must start live — a quarantined definition is skipped by the fuse check"
    );

    let mut client_a = db.pool.get().await.expect("pool connection for worker a");
    let mut client_b = db.pool.get().await.expect("pool connection for worker b");

    // Worker A: one more eviction, checked under the *bare* spelling.
    let txn_a = client_a.transaction().await.expect("begin worker a");
    poison_in_txn(&txn_a, &qualified, "concurrent-a").await;
    trip_transform_fuse_if_crossed(&txn_a, &db.pool, bare)
        .await
        .expect("worker a's fuse check");
    assert_eq!(
        transform_status(&client, "order_totals").await,
        "live",
        "worker a alone only reaches `threshold - 1` visible keys, so it must not trip the fuse"
    );

    let mut worker_b_blocked = false;
    let worker_b = async {
        let txn_b = client_b.transaction().await.expect("begin worker b");
        poison_in_txn(&txn_b, &qualified, "concurrent-b").await;
        // The *qualified* spelling, against worker A's bare one.
        trip_transform_fuse_if_crossed(&txn_b, &db.pool, &qualified)
            .await
            .expect("worker b's fuse check");
        txn_b.commit().await.expect("commit worker b");
    };
    let release_worker_a = async {
        for _ in 0..200 {
            if backends_waiting_on_a_lock(&client).await > 0 {
                worker_b_blocked = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        txn_a.commit().await.expect("commit worker a");
    };
    tokio::join!(worker_b, release_worker_a);

    assert!(
        worker_b_blocked,
        "worker b must have parked on the one shared gate row while worker a held it — two \
         spellings of one source that take two different lock rows serialize against nothing \
         (issues #159, #283)"
    );
    assert_eq!(
        transform_status(&client, "order_totals").await,
        "quarantined",
        "the second worker through the gate must count both spellings' evictions and trip the fuse"
    );
}

/// `V33__quarantine_canonical_src_table.sql` must fold pre-existing bare rows
/// into their qualified counterpart: `key_deaths.deaths` **summed**, the marker
/// tables deduplicated, and every bare row gone afterwards.
///
/// Runs the migration's own SQL text (not a paraphrase of it) a second time,
/// against dual-spelling rows staged directly — the migration itself has of
/// course already run against this database, and every statement in it is
/// idempotent and re-runnable by construction, which is what makes replaying it
/// a legitimate way to test the fold. The `create temporary table` it opens with
/// is local to this connection and dropped again at the end of the script.
#[tokio::test]
async fn the_v33_fold_combines_dual_spelling_quarantine_rows() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;
    let qualified = qualify_fixture_table("orders");
    let bare = "orders";

    // A key poisoned under both spellings; a key poisoned only bare.
    insert_poison_marker(&client, &qualified, "both").await;
    insert_poison_marker(&client, bare, "both").await;
    insert_poison_marker(&client, bare, "bare-only").await;

    // Death counters to sum: 4 + 3 for the shared key, 2 for the bare-only one.
    client
        .execute(
            "insert into key_deaths (src_table, key, deaths, last_error, last_death_at) values \
                 ($1, 'both', 4, 'from the qualified row', now() - interval '1 hour'), \
                 ($2, 'both', 3, 'from the bare row', now()), \
                 ($2, 'bare-only', 2, 'bare only', now())",
            &[&qualified, &bare],
        )
        .await
        .expect("seed dual-spelling key_deaths rows");

    // Held work under the bare spelling only, for the already-qualified key:
    // the fold has to carry it across or `release_key` would never find it.
    insert_poison_held(
        &client,
        bare,
        "both",
        1,
        "insert",
        None,
        Some(r#"{"price":"1.00","tax":"0.10"}"#),
        Some(1),
    )
    .await;

    // The gate's own bookkeeping, one row per spelling.
    client
        .execute(
            "insert into transform_fuse_gate (src_table, checks) values ($1, 7), ($2, 5)",
            &[&qualified, &bare],
        )
        .await
        .expect("seed dual-spelling gate rows");

    // One `column_failures` row under each spelling for the same physical row
    // and the same `(transform, column)` — the split dedup that let one
    // stubborn row charge `column_deaths` twice.
    client
        .execute(
            "insert into column_failures (transform_table, column_name, src_table, key, error) \
             values ('public.order_totals', 'total', $1, 'both', 'qualified'), \
                    ('public.order_totals', 'total', $2, 'both', 'bare')",
            &[&qualified, &bare],
        )
        .await
        .expect("seed dual-spelling column_failures rows");

    client
        .batch_execute(include_str!(
            "../migrations/V33__quarantine_canonical_src_table.sql"
        ))
        .await
        .expect("replay the V33 fold");

    for table in [
        "poison",
        "poison_held",
        "key_deaths",
        "column_failures",
        "transform_fuse_gate",
    ] {
        let leftover: i64 = client
            .query_one(
                &format!("select count(*) from {table} where src_table = $1"),
                &[&bare],
            )
            .await
            .expect("count bare rows")
            .get(0);
        assert_eq!(
            leftover, 0,
            "{table} must hold no bare-spelled rows once the fold has run"
        );
    }

    assert_eq!(
        poison_rows_for(&client, &qualified).await,
        2,
        "the shared key must have deduplicated and the bare-only key must have been carried across"
    );
    assert!(
        poison_marker_exists(&client, &qualified, "bare-only").await,
        "a key poisoned only under the bare spelling must survive the fold, qualified"
    );

    assert_eq!(
        key_deaths_count(&client, &qualified, "both").await,
        Some(7),
        "the two split death counters for one physical row must be summed, not clobbered"
    );
    assert_eq!(
        key_deaths_count(&client, &qualified, "bare-only").await,
        Some(2),
        "a counter that only ever existed bare must be carried across unchanged"
    );
    let last_error: String = client
        .query_one(
            "select last_error from key_deaths where src_table = $1 and key = 'both'",
            &[&qualified],
        )
        .await
        .expect("read the folded last_error")
        .get(0);
    assert_eq!(
        last_error, "from the bare row",
        "the surviving diagnostic must be whichever spelling's row died more recently"
    );

    let held: i64 = client
        .query_one(
            "select count(*) from poison_held where src_table = $1 and key = 'both'",
            &[&qualified],
        )
        .await
        .expect("count folded held rows")
        .get(0);
    assert_eq!(held, 1, "the bare row's parked work must be carried across");

    let (checks, gate_rows): (i64, i64) = {
        let row = client
            .query_one(
                "select (select checks from transform_fuse_gate where src_table = $1), \
                        (select count(*) from transform_fuse_gate)",
                &[&qualified],
            )
            .await
            .expect("read the folded gate row");
        (row.get(0), row.get(1))
    };
    assert_eq!(
        gate_rows, 1,
        "one lock row per logical source, not per spelling"
    );
    assert_eq!(checks, 12, "the gate's check bookkeeping must combine");

    let failures: i64 = client
        .query_one(
            "select count(*) from column_failures where key = 'both'",
            &[],
        )
        .await
        .expect("count folded column_failures rows")
        .get(0);
    assert_eq!(
        failures, 1,
        "the split dedup key must collapse to one row for one physical row (issue #283) — this is \
         the one place the dual spelling over-counted rather than under-counted"
    );
}
