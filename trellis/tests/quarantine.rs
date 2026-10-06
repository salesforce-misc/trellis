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

#[path = "support/drain_driver.rs"]
mod drain_driver;

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
use trellis::staging::{
    ChargedKey, FoldedChange, IsolationOutcome, LastChange, StagedWatermark, isolate_and_evict,
};

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
    let lsn = testkit::wal_insert_lsn(client).await;
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

/// The id of the definition whose bare target is `target`: whole-key poison
/// is keyed by it (#799).
async fn transform_id(client: &Client, target: &str) -> i64 {
    client
        .query_one(
            "select id from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .unwrap_or_else(|e| panic!("no definition {target:?}: {e}"))
        .get(0)
}

/// Marks `(src_table, key)` poisoned for the definition `transform`.
async fn insert_poison_marker(client: &Client, transform: &str, src_table: &str, key: &str) {
    let id = transform_id(client, transform).await;
    client
        .execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             values ($1, $2, $3, 'test')",
            &[&id, &src_table, &key],
        )
        .await
        .expect("insert poison marker");
}

#[allow(clippy::too_many_arguments)]
async fn insert_poison_held(
    client: &Client,
    transform: &str,
    src_table: &str,
    key: &str,
    seg_seq: i64,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
    origin_lsn: Option<u64>,
) {
    let id = transform_id(client, transform).await;
    client
        .execute(
            "insert into poison_held \
                 (transform_id, src_table, key, seg_seq, op, lsn, old_image, new_image, \
                  origin_lsn, hop_gen) \
             values ($9, $1, $2, $3, $4, $5, $6::text::jsonb, $7::text::jsonb, $8, 0)",
            &[
                &src_table,
                &key,
                &seg_seq,
                &op,
                &Some(testkit::wal_insert_lsn(client).await),
                &old_image,
                &new_image,
                &origin_lsn.map(PgLsn::from),
                &id,
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
    // capture runs in this test file, and it isn't exercising guard (a).
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

    seed_order_totals(&db, &client).await;
    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed orders row");

    insert_poison_marker(&client, "order_totals", "orders", "1").await;
    insert_poison_held(
        &client,
        "order_totals",
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

    let replayed = trellis::staging::release_key(&db.pool, "order_totals", "orders", "1")
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
            "insert into key_deaths (transform_id, src_table, key, deaths, last_error) \
             select id, $1, '1', 3, 'earlier isolate attempt' from transform_definitions",
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
    insert_poison_marker(&client, "order_totals", &orders, "2").await;

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

    let replayed = trellis::staging::release_key(&db.pool, "order_totals", &orders, "2")
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

/// A second definition reading `order_totals`, so it has a downstream
/// reader of its own — required for the hop-bound check to trip at all.
async fn seed_order_summary(db: &TestDatabase, def: &TransformDef) {
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
}

/// Scenario: a halting schema diagnosis (the hop bound) must never be
/// quarantined, regardless of how few or many keys it touches. Issue #663:
/// it pauses the definitions the runaway wave reaches (`order_summary`,
/// which reads the table it ran away through), records why and counts one
/// halting stop, and the rest of the page commits without them.
#[tokio::test]
async fn a_halting_schema_error_is_never_quarantined_and_pauses_what_it_reaches() {
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

    seed_order_summary(&db, &def).await;

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
        Ok(Some(outcome)) => assert!(outcome.batch_drained, "the page drains"),
        other => panic!("expected the halt to pause what it reaches and drain, got {other:?}"),
    }

    assert!(
        !poison_marker_exists(&client, &orders, "1").await,
        "a halting schema error must never quarantine the key that triggered it"
    );

    let after = trellis::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats after");
    assert_eq!(after.stop_count, before.stop_count + 1);
    assert!(
        after
            .last_reason
            .as_deref()
            .is_some_and(|r| r.contains("hop bound")),
        "{:?}",
        after.last_reason
    );

    let halted = client
        .query(
            "select split_part(d.target_table, '.', 2), d.status, f.kind \
             from transform_definitions d \
             left join capture_failures f on f.transform_id = d.id order by d.id",
            &[],
        )
        .await
        .expect("read statuses");
    let halted: Vec<(String, String, Option<String>)> = halted
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    let (_, summary_status, summary_kind) = halted
        .iter()
        .find(|(target, ..)| target == "order_summary")
        .expect("order_summary");
    assert_eq!(summary_status, "paused", "{halted:?}");
    assert_eq!(summary_kind.as_deref(), Some("halt"), "{halted:?}");
    let (_, totals_status, totals_kind) = halted
        .iter()
        .find(|(target, ..)| target == "order_totals")
        .expect("order_totals");
    assert_ne!(totals_status, "paused", "{halted:?}");
    assert_eq!(*totals_kind, None, "{halted:?}");

    let written: i64 = client
        .query_one("select count(*) from order_totals where id = 1", &[])
        .await
        .expect("count order_totals rows")
        .get(0);
    assert_eq!(
        written, 1,
        "the retry without the paused reader applies the page's own write"
    );
}

/// Issue #663: a halting failure only an isolation probe meets is halted on
/// like the page's own. The page fails on key 2, whose total breaks a check
/// on `order_totals` before propagation runs, so isolation probes it. The
/// probe of key 1 alone writes, then runs past the hop bound: that pauses
/// `order_summary`, counted as one stop, and the page retries without it.
/// Key 2 alone keeps failing, so it is charged on each drain, then evicted,
/// and the page drains with key 1's write. The stop isn't counted again.
#[tokio::test]
async fn a_halting_failure_an_isolation_probe_meets_pauses_what_it_reaches() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let def = seed_order_totals(&db, &client).await;
    seed_order_summary(&db, &def).await;
    client
        .batch_execute("alter table order_totals add constraint small check (total < 1000)")
        .await
        .expect("constrain order_totals");
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
    insert_cdc_row(
        &client,
        "seg_0",
        &orders,
        "2",
        "insert",
        None,
        Some(r#"{"price":"5000.00","tax":"1.00"}"#),
    )
    .await;
    let before = trellis::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats before");

    let seg_seq = seal_active_segment(&mut client).await;
    let mut drains = 0;
    loop {
        drains += 1;
        assert!(drains <= 10, "the page never drained");
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
            Ok(_) => break,
            Err(err) => assert!(
                err.to_string().contains("small"),
                "only the check on key 2 surfaces: {err}"
            ),
        }
        let after = trellis::staging::halting_stop_stats(&db.pool)
            .await
            .expect("halting_stop_stats");
        assert_eq!(after.stop_count, before.stop_count + 1, "drain {drains}");
    }
    assert!(segment_state_is_drained(&client, seg_seq).await);
    assert_eq!(transform_status(&client, "order_summary").await, "paused");
    let kind: String = client
        .query_one(
            "select f.kind from capture_failures f \
             join transform_definitions d on d.id = f.transform_id \
             where split_part(d.target_table, '.', 2) = 'order_summary'",
            &[],
        )
        .await
        .expect("order_summary's record")
        .get(0);
    assert_eq!(kind, "halt");
    assert_ne!(transform_status(&client, "order_totals").await, "paused");
    let after = trellis::staging::halting_stop_stats(&db.pool)
        .await
        .expect("halting_stop_stats after");
    assert_eq!(after.stop_count, before.stop_count + 1);
    assert!(
        poison_marker_exists(&client, &orders, "2").await,
        "key 2 evicted"
    );
    assert!(!poison_marker_exists(&client, &orders, "1").await);
    let written: i64 = client
        .query_one("select count(*) from order_totals where id = 1", &[])
        .await
        .expect("count order_totals rows")
        .get(0);
    assert_eq!(written, 1, "key 1 applied once its reader paused");
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
    // enumeration (`intake::markers::enumerate_and_append`) stages this
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
            "create table ledger (entry_id numeric primary key, account text, cents integer)",
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
        .batch_execute("create table sales (id integer primary key, price numeric, qty integer)")
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
    trellis::intake::markers::settle_registrations(&db.pool).await;
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

/// Scenario: `poison_held` is idempotent on `(transform_id, src_table, key,
/// seg_seq)` — a retried park for the exact same definition, batch and key
/// must not duplicate or overwrite the row already parked for it.
#[tokio::test]
async fn poison_held_is_idempotent_on_table_key_batch() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;

    let insert = "insert into poison_held \
                      (transform_id, src_table, key, seg_seq, op, old_image, new_image, hop_gen) \
                  select id, $1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0 \
                  from transform_definitions \
                  on conflict (transform_id, src_table, key, seg_seq) do nothing";

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
    insert_poison_marker(&client, "order_totals", &orders, "1").await;
    // Two excluding batches' own parked contributions for the same key,
    // inserted out of seg_seq order here to prove release doesn't just
    // replay insertion order.
    insert_poison_held(
        &client,
        "order_totals",
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
        "order_totals",
        &orders,
        "1",
        1,
        "update",
        None,
        Some(r#"{"price":"10.00","tax":"1.00"}"#),
        Some(10),
    )
    .await;

    let replayed = trellis::staging::release_key(&db.pool, "order_totals", &orders, "1")
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
            "insert into key_deaths (transform_id, src_table, key, deaths, last_error) \
             select id, 'orders', '1', 10, 'earlier isolate attempts' from transform_definitions",
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
        hop_gen: 0,
        first_seen: SystemTime::now(),
        group_key: None,
        is_truncate: false,
        relationship_reverse_deferred: None,
        retry_count: 0,
        prior_image: None,
        row_count: 1,
        has_recompute: false,
        ends_in_delete: false,
        last_change: None,
        to_col_values: Vec::new(),
    }];

    let result = isolate_and_evict(&db.pool, 1, "worker", "trellis_quarantine_test", &folded, 0)
        .await
        .expect("isolate_and_evict with threshold 0 must not error");
    assert!(
        matches!(result, IsolationOutcome::FuseDisabled),
        "threshold 0 must never evict, regardless of how many deaths a key already has, \
         got {result:?}"
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
        hop_gen: 0,
        first_seen: SystemTime::now(),
        group_key: None,
        is_truncate: false,
        relationship_reverse_deferred: Some(999),
        retry_count: 1,
        prior_image: None,
        row_count: 1,
        has_recompute: false,
        ends_in_delete: false,
        last_change: None,
        to_col_values: Vec::new(),
    }];

    let result = isolate_and_evict(&db.pool, 1, "worker", "trellis_quarantine_test", &folded, 1)
        .await
        .expect(
            "isolate_and_evict must not error on a deferred reverse — it must be skipped \
             outright, never probed",
        );
    assert!(
        matches!(result, IsolationOutcome::NothingReproduced),
        "a batch containing only a deferred reverse must report nothing reproduced, so the \
         caller surfaces the original failure instead of silently 'resolving' it, got {result:?}"
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
async fn poison_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    transform_id: i64,
    src_table: &str,
    key: &str,
) {
    txn.execute(
        "insert into poison (transform_id, src_table, key, last_error) \
         values ($1, $2, $3, 'test')",
        &[&transform_id, &src_table, &key],
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
    let id = transform_id(&client, "order_totals").await;

    // Already committed: two short of the threshold, so neither transaction
    // below can cross it on the strength of its own single new eviction.
    for i in 0..(DEFAULT_TRANSFORM_DEATH_THRESHOLD - 2) {
        insert_poison_marker(&client, "order_totals", &orders, &format!("settled-{i}")).await;
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
    poison_in_txn(&txn_a, id, &orders, "concurrent-a").await;
    trip_transform_fuse_if_crossed(&txn_a, id)
        .await
        .expect("worker a's fuse check");
    assert_eq!(
        transform_status(&client, "order_totals").await,
        "live",
        "worker a alone only reaches `threshold - 1` visible keys, so it must not trip the fuse"
    );

    let worker_b = async {
        let txn_b = client_b.transaction().await.expect("begin worker b");
        poison_in_txn(&txn_b, id, &orders, "concurrent-b").await;
        trip_transform_fuse_if_crossed(&txn_b, id)
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
        hop_gen: 0,
        first_seen: SystemTime::now(),
        group_key: None,
        is_truncate: false,
        relationship_reverse_deferred: None,
        retry_count: 0,
        prior_image: None,
        row_count: 1,
        has_recompute: false,
        ends_in_delete: false,
        // #623 D6: an Apply (its image is the change), not a Re-derive.
        last_change: Some(LastChange {
            lsn: PgLsn::from(1),
            row_txid: "1".to_string(),
        }),
        to_col_values: Vec::new(),
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
        let IsolationOutcome::Evicted { evicted, .. } = retry else {
            panic!(
                "the key staged under {src_table:?} must have been evicted at threshold 1, \
                 got {retry:?}"
            )
        };
        assert_eq!(evicted, 1, "the batch's only key, for its only reader");
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

/// Issue #614: a key that reproduces the failure alone but is still below the
/// death threshold must come back as `ChargedBelowThreshold`, naming the key and
/// its death count — not as the same "nothing reproduced" answer a batch with no
/// attributable key gets. Pre-fix both returned `Ok(None)`, so
/// `classify_and_retry` logged "isolation reproduced nothing" for a key that was
/// in fact a few drain cycles from eviction. The second call then shows the
/// count carrying across calls (one charge per call) up to the eviction.
#[tokio::test]
async fn below_threshold_charge_is_reported_distinctly_from_nothing_reproduced() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;

    let qualified = qualify_fixture_table("orders");
    let folded = vec![unevaluable_change(&qualified, "7")];

    let first = isolate_and_evict(&db.pool, 1, "worker", "trellis_quarantine_test", &folded, 2)
        .await
        .expect("first isolate_and_evict");
    let IsolationOutcome::ChargedBelowThreshold { charged } = first else {
        panic!("a reproducing key at 1 of 2 deaths must be reported as charged, got {first:?}")
    };
    assert_eq!(
        charged,
        vec![ChargedKey {
            transform: "order_totals".to_string(),
            src_table: qualified.clone(),
            key: "7".to_string(),
            deaths: 1,
        }]
    );
    assert!(!poison_marker_exists(&client, &qualified, "7").await);

    let second = isolate_and_evict(&db.pool, 1, "worker", "trellis_quarantine_test", &folded, 2)
        .await
        .expect("second isolate_and_evict");
    let IsolationOutcome::Evicted { evicted, charged } = second else {
        panic!("the second charge reaches the threshold of 2 and must evict, got {second:?}")
    };
    assert_eq!(evicted, 1, "the only key was evicted");
    assert!(charged.is_empty(), "no other key was charged");
    assert!(poison_marker_exists(&client, &qualified, "7").await);
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

// ---------------------------------------------------------------------
// Issue #655: isolation bisects a failed page instead of probing it one
// record at a time.
// ---------------------------------------------------------------------

/// How many records the large-page isolation tests stage: big enough that
/// probing every record alone (the pre-#655 isolation) runs for minutes, small
/// enough that the bisection's two probes per level stay quick.
const LARGE_PAGE: i32 = 4096;

/// Stages `orders` inserts for keys `1..=LARGE_PAGE` into the ring in one
/// statement, each with a valid price except the keys in `poisoned`, whose
/// price is not a number, and seals them into one batch.
async fn stage_large_page_with_poisoned(client: &mut Client, poisoned: &[i32]) -> (String, i64) {
    stage_large_page(client, poisoned, "not-a-number").await
}

/// [`stage_large_page_with_poisoned`], with `poisoned_price` as the poisoned
/// keys' price.
async fn stage_large_page(
    client: &mut Client,
    poisoned: &[i32],
    poisoned_price: &str,
) -> (String, i64) {
    let orders = qualify_fixture_table("orders");
    client
        .execute(
            "insert into seg_0 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
             select $1, g::text, 'insert', pg_current_wal_insert_lsn(), null, \
                    jsonb_build_object( \
                        'price', case when g = any($2) then $4 else '1.00' end, \
                        'tax', '1.00'), \
                    0 \
             from generate_series(1, $3) as g",
            &[&orders, &poisoned, &LARGE_PAGE, &poisoned_price],
        )
        .await
        .expect("stage the large page");
    let seg_seq = seal_active_segment(client).await;
    (orders, seg_seq)
}

/// `drain_once` for a batch expected to fail: the error it surfaces, or
/// whatever it returned instead.
async fn drain_result(
    pool: &trellis::Pool,
    seg_seq: i64,
) -> Result<Option<apply::ApplyOutcome>, ApplyError> {
    apply::drain_once(
        pool,
        seg_seq,
        "worker",
        1,
        "trellis_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
}

async fn charged_keys(client: &Client, src_table: &str) -> Vec<(String, i32)> {
    client
        .query(
            "select key, deaths from key_deaths where src_table = $1 order by key::int",
            &[&src_table],
        )
        .await
        .expect("read key_deaths")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// Issue #655: one poisoned key, the only failure in a large page, is found and
/// charged alone, and nothing else in the page is charged. Before #655 this
/// probed all 4,096 records one at a time (a compute plus a rollback-only apply
/// each); bisection gets there in about two probes per halving.
#[tokio::test]
async fn isolation_bisects_a_large_page_to_its_one_poisoned_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;

    let (orders, seg_seq) = stage_large_page_with_poisoned(&mut client, &[2731]).await;
    let started = std::time::Instant::now();
    let result = drain_result(&db.pool, seg_seq).await;
    eprintln!(
        "isolating a {LARGE_PAGE}-record page took {:?}",
        started.elapsed()
    );
    match result {
        Err(ApplyError::Eval(_)) => {}
        other => panic!("expected the poisoned key's Eval error to surface, got {other:?}"),
    }
    assert_eq!(
        charged_keys(&client, &orders).await,
        vec![("2731".to_string(), 1)]
    );
    assert!(!segment_state_is_drained(&client, seg_seq).await);
}

/// Issue #655: two poisoned keys in one large page are both found and charged,
/// each once, and no batch-mate is.
#[tokio::test]
async fn isolation_bisects_a_large_page_to_both_poisoned_keys() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;

    let (orders, seg_seq) = stage_large_page_with_poisoned(&mut client, &[17, 3900]).await;
    match drain_result(&db.pool, seg_seq).await {
        Err(ApplyError::Eval(_)) => {}
        other => panic!("expected a poisoned key's Eval error to surface, got {other:?}"),
    }
    assert_eq!(
        charged_keys(&client, &orders).await,
        vec![("17".to_string(), 1), ("3900".to_string(), 1)]
    );
}

/// Issue #655: the same, for a failure only the apply hits (a check constraint
/// on the target), so every probe of a half page computes and applies that
/// half before rolling back.
#[tokio::test]
async fn isolation_bisects_a_large_page_to_a_key_that_fails_on_apply() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;
    client
        .batch_execute(
            "alter table order_totals add constraint total_below_1000 check (total < 1000)",
        )
        .await
        .expect("add the check constraint the poisoned key trips");
    // The live rows the 1-1 apply reads, matching the staged images.
    client
        .batch_execute(&format!(
            "insert into orders (id, price, tax) \
             select g, case when g = 1234 then 5000.00 else 1.00 end, 1.00 \
             from generate_series(1, {LARGE_PAGE}) as g"
        ))
        .await
        .expect("seed live orders rows");

    let (orders, seg_seq) = stage_large_page(&mut client, &[1234], "5000.00").await;
    let started = std::time::Instant::now();
    let result = drain_result(&db.pool, seg_seq).await;
    eprintln!(
        "isolating a {LARGE_PAGE}-record page on apply took {:?}",
        started.elapsed()
    );
    match result {
        Err(ApplyError::Db(err)) => assert_eq!(err.code(), Some(&SqlState::CHECK_VIOLATION)),
        other => panic!("expected the check violation to surface, got {other:?}"),
    }
    assert_eq!(
        charged_keys(&client, &orders).await,
        vec![("1234".to_string(), 1)]
    );
}

/// Issue #655 review: a key that fails alone can be masked inside a run by a
/// batch-mate. Here three keys share one brand-new aggregate group under a
/// `total <= 8` check: key 1 adds 10 (fails alone), key 2 subtracts 5 and
/// key 3 adds 5, and key 4 is in another group. The page fails (10), but
/// its halves `{1, 2}` (5) and `{3, 4}` (5 and 1) both pass: the failure
/// needs records from both halves, and key 2 masks key 1 inside its half.
/// Isolation must still pin key 1, which fails alone, as probing every
/// record alone did before #655, or nothing is ever charged and the page
/// stays wedged. Evicting key 1 is what lets the page drain.
#[tokio::test]
async fn isolation_pins_a_key_masked_by_a_batch_mate_in_its_half() {
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
        .expect("add the check constraint");
    client
        .batch_execute(
            "insert into order_items (id, order_id, amount) values \
             (1, 1, 10.00), (2, 1, -5.00), (3, 1, 5.00), (4, 2, 1.00)",
        )
        .await
        .expect("seed live order_items rows");

    let order_items = qualify_fixture_table("order_items");
    for (key, image) in [
        ("1", r#"{"order_id":"1","amount":"10.00"}"#),
        ("2", r#"{"order_id":"1","amount":"-5.00"}"#),
        ("3", r#"{"order_id":"1","amount":"5.00"}"#),
        ("4", r#"{"order_id":"2","amount":"1.00"}"#),
    ] {
        insert_cdc_row(
            &client,
            "seg_0",
            &order_items,
            key,
            "insert",
            None,
            Some(image),
        )
        .await;
    }
    let seg_seq = seal_active_segment(&mut client).await;

    match drain_result(&db.pool, seg_seq).await {
        Err(ApplyError::Db(err)) => assert_eq!(err.code(), Some(&SqlState::CHECK_VIOLATION)),
        other => panic!("expected the check violation to surface, got {other:?}"),
    }
    assert_eq!(
        charged_keys(&client, &order_items).await,
        vec![("1".to_string(), 1)],
        "key 1 fails alone and must be charged, masked or not; nothing else is"
    );

    let mut failures = 1;
    let outcome = loop {
        match drain_result(&db.pool, seg_seq).await {
            Ok(Some(outcome)) => break outcome,
            Ok(None) => panic!("drain_once claimed nothing on a still-undrained segment"),
            Err(_) => {
                failures += 1;
                assert!(
                    failures <= 20,
                    "key 1 was never evicted; the page stays wedged"
                );
            }
        }
    };
    assert_eq!(
        outcome.keys_written, 2,
        "both groups are written once key 1 is evicted"
    );
    assert!(poison_marker_exists(&client, &order_items, "1").await);
    assert!(segment_state_is_drained(&client, seg_seq).await);
    let totals: Vec<(i32, String)> = client
        .query(
            "select order_id::int, total::text from order_summary order by order_id",
            &[],
        )
        .await
        .expect("read order_summary")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        totals,
        vec![(1, "0.00".to_string()), (2, "1.00".to_string())],
        "keys 2, 3 and 4 applied; key 1 is held"
    );
}

// ---------------------------------------------------------------------
// Issue #799: whole-key poison is per transform. One definition's key
// failure holds the key for that definition only; every other reader of
// the source keeps applying it.
// ---------------------------------------------------------------------

/// [`seed_order_totals`] plus a second reader of `orders`, `order_prices`,
/// whose target refuses a price of 100 or more: a constraint on its target,
/// which fails `order_prices`' write for such a key and nobody else's.
async fn seed_two_readers(db: &TestDatabase, client: &Client) {
    let def = seed_order_totals(db, client).await;
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    let prices = create_definition(
        &db.pool,
        "TRANSFORM order_prices FROM orders SELECT price AS price",
        &source_columns,
    )
    .await
    .expect("create order_prices definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(
        &db.pool,
        &prices.def,
        "public",
        &pk,
        &source_columns,
        &prices.def.source,
    )
    .await
    .expect("create order_prices table");
    client
        .batch_execute("alter table public.order_prices add constraint cheap check (price < 100)")
        .await
        .expect("constrain order_prices");
}

/// The ring table writers currently append to.
async fn active_ring_table(client: &Client) -> String {
    let slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment_pointer")
        .get(0);
    format!("seg_{slot}")
}

/// Writes `orders` row `(id, price, tax)` and stages its CDC insert or
/// update into the active ring table, as capture would.
async fn write_order(client: &Client, id: i32, price: &str, tax: &str) {
    let existed = client
        .query_opt("select 1 from orders where id = $1", &[&id])
        .await
        .expect("read orders")
        .is_some();
    client
        .execute(
            "insert into orders (id, price, tax) values ($1, $2::text::numeric, $3::text::numeric) \
             on conflict (id) do update set price = excluded.price, tax = excluded.tax",
            &[&id, &price, &tax],
        )
        .await
        .expect("write the orders row");
    let image = format!(r#"{{"id":"{id}","price":"{price}","tax":"{tax}"}}"#);
    let (op, old_image) = if existed {
        ("update", Some(image.as_str()))
    } else {
        ("insert", None)
    };
    insert_cdc_row(
        client,
        &active_ring_table(client).await,
        &qualify_fixture_table("orders"),
        &id.to_string(),
        op,
        old_image,
        Some(&image),
    )
    .await;
}

/// Seals the active segment and drains it until the page commits. Each failed
/// drain charges every key that fails alone one death, and the drain whose
/// charge crosses the threshold evicts the key and commits the rest of the
/// page. Returns how many drains failed first.
async fn drain_through_evictions(client: &mut Client, pool: &trellis::Pool) -> usize {
    let seg_seq = seal_active_segment(client).await;
    let mut failures = 0;
    loop {
        match drain_result(pool, seg_seq).await {
            Ok(Some(_)) => return failures,
            Ok(None) => panic!("drain_once claimed nothing on a still-undrained segment"),
            Err(_) => {
                failures += 1;
                assert!(failures <= 20, "the page never committed");
            }
        }
    }
}

/// `(target, src_table, key)` for every `poison` row, in that order.
async fn poisoned_for(client: &Client) -> Vec<(String, String, String)> {
    client
        .query(
            "select split_part(d.target_table, '.', 2), p.src_table, p.key \
             from poison p join transform_definitions d on d.id = p.transform_id \
             order by 1, 2, 3",
            &[],
        )
        .await
        .expect("read poison")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

/// The single text value `sql` returns, or `None` for no row.
async fn text_of(client: &Client, sql: &str) -> Option<String> {
    client
        .query_opt(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .map(|row| row.get(0))
}

/// How many rows of `table` (a quarantine table) belong to `target`.
async fn rows_for(client: &Client, table: &str, target: &str) -> i64 {
    client
        .query_one(
            &format!(
                "select count(*) from {table} t join transform_definitions d \
                 on d.id = t.transform_id where split_part(d.target_table, '.', 2) = $1"
            ),
            &[&target],
        )
        .await
        .expect("count quarantine rows")
        .get(0)
}

/// The blast radius (#799): `order_prices`' write of key 1 fails (its target
/// refuses the price), so isolation charges the key to `order_prices` alone
/// and, at the death threshold, holds it there. `order_totals` keeps applying
/// key 1, both the change that failed and a later one, while `order_prices`
/// parks them.
async fn hold_key_1_for_order_prices(db: &TestDatabase, client: &mut Client) {
    seed_two_readers(db, client).await;
    write_order(client, 1, "500", "2").await;
    write_order(client, 2, "20", "1").await;
    let failures = drain_through_evictions(client, &db.pool).await;
    assert_eq!(
        failures,
        trellis::staging::DEFAULT_DEATH_THRESHOLD as usize - 1,
        "every drain below the threshold surfaces the failure, and the one that crosses it \
         evicts the key and commits"
    );
}

#[tokio::test]
async fn a_key_one_definition_fails_on_stays_live_in_every_other_reader() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    hold_key_1_for_order_prices(&db, &mut client).await;
    let orders = qualify_fixture_table("orders");

    assert_eq!(
        poisoned_for(&client).await,
        vec![("order_prices".to_string(), orders.clone(), "1".to_string())],
        "the key is held for the definition whose write failed, and only for it"
    );
    assert_eq!(
        text_of(&client, "select total::text from order_totals where id = 1").await,
        Some("502".to_string()),
        "order_totals applied the change order_prices failed on"
    );
    assert_eq!(
        text_of(&client, "select price::text from order_prices where id = 1").await,
        None,
        "order_prices holds key 1"
    );
    assert_eq!(
        text_of(&client, "select price::text from order_prices where id = 2").await,
        Some("20".to_string()),
        "the rest of the page applied to order_prices"
    );
    assert_eq!(
        rows_for(&client, "key_deaths", "order_totals").await,
        0,
        "order_totals was never charged"
    );

    // A later change to key 1 reaches order_totals and is parked for
    // order_prices.
    write_order(&client, 1, "600", "3").await;
    assert_eq!(drain_through_evictions(&mut client, &db.pool).await, 0);
    assert_eq!(
        text_of(&client, "select total::text from order_totals where id = 1").await,
        Some("603".to_string()),
        "order_totals keeps updating key 1"
    );
    assert_eq!(
        text_of(&client, "select price::text from order_prices where id = 1").await,
        None
    );
    assert_eq!(
        rows_for(&client, "poison_held", "order_prices").await,
        2,
        "the evicting page's change and the later one are both held for order_prices"
    );
    assert_eq!(rows_for(&client, "poison_held", "order_totals").await, 0);
}

/// Rule 6 (#799): a wait scoped to a definition gates on its own held keys
/// only. `order_prices` holds key 1, so a cluster-wide wait and a wait on
/// `order_prices` don't converge, while one on `order_totals` does.
#[tokio::test]
async fn a_held_key_gates_its_own_definitions_wait_and_not_a_siblings() {
    use std::time::Duration;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    hold_key_1_for_order_prices(&db, &mut client).await;
    let token = converge::watermark_token(&client)
        .await
        .expect("watermark token");

    let totals = transform_id(&client, "order_totals").await;
    let prices = transform_id(&client, "order_prices").await;
    converge::await_converged_for(&client, token, Duration::ZERO, totals)
        .await
        .expect("order_totals holds nothing, so its wait converges");
    assert!(
        converge::await_converged_for(&client, token, Duration::ZERO, prices)
            .await
            .is_err(),
        "order_prices' held key gates its own wait"
    );
    assert!(
        converge::await_converged(&client, token, Duration::ZERO)
            .await
            .is_err(),
        "a cluster-wide wait counts every definition's held keys"
    );
}

/// Resume (#799 rule 5): resuming `order_prices` deletes its own `poison`,
/// `poison_held` and `key_deaths` rows before its fresh build, and leaves the
/// key `order_totals` holds exactly as it was.
#[tokio::test]
async fn resuming_a_definition_releases_its_own_held_keys_only() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    hold_key_1_for_order_prices(&db, &mut client).await;
    let orders = qualify_fixture_table("orders");

    // order_totals holds key 2, with parked work of its own.
    insert_poison_marker(&client, "order_totals", &orders, "2").await;
    insert_poison_held(
        &client,
        "order_totals",
        &orders,
        "2",
        1,
        "update",
        None,
        Some(r#"{"price":"20","tax":"1"}"#),
        Some(10),
    )
    .await;
    assert!(rows_for(&client, "key_deaths", "order_prices").await > 0);

    // Frozen by hand, as `PAUSE TRANSFORM` freezes it.
    client
        .batch_execute(
            "update transform_definitions set status = 'paused' \
             where split_part(target_table, '.', 2) = 'order_prices'",
        )
        .await
        .expect("pause order_prices");
    trellis::staging::quarantine::resume_transform(&db.pool, "order_prices")
        .await
        .expect("resume order_prices");

    for table in ["poison", "poison_held", "key_deaths"] {
        assert_eq!(
            rows_for(&client, table, "order_prices").await,
            0,
            "{table}: the resume releases every key order_prices held"
        );
    }
    assert_eq!(
        poisoned_for(&client).await,
        vec![("order_totals".to_string(), orders, "2".to_string())],
        "order_totals' held key is untouched"
    );
    assert_eq!(rows_for(&client, "poison_held", "order_totals").await, 1);
}

/// Release (#799 rule 4): releasing `order_prices`' key 1, once its cause is
/// gone, stages a `Recompute` that re-derives the key for `order_prices` from
/// the live row. It reaches `order_totals` too, as an idempotent re-derive,
/// which leaves its row as it was.
#[tokio::test]
async fn releasing_a_key_re_derives_it_for_its_definition_and_leaves_a_sibling_unchanged() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    hold_key_1_for_order_prices(&db, &mut client).await;
    let orders = qualify_fixture_table("orders");

    client
        .batch_execute("alter table public.order_prices drop constraint cheap")
        .await
        .expect("fix the cause");
    let released = trellis::staging::release_key(&db.pool, "order_prices", &orders, "1")
        .await
        .expect("release key 1 for order_prices");
    assert_eq!(released, 1, "the evicting page's change was held");
    assert_eq!(poisoned_for(&client).await, vec![]);
    for table in ["poison_held", "key_deaths"] {
        assert_eq!(rows_for(&client, table, "order_prices").await, 0, "{table}");
    }

    assert_eq!(drain_through_evictions(&mut client, &db.pool).await, 0);
    assert_eq!(
        text_of(&client, "select price::text from order_prices where id = 1").await,
        Some("500".to_string()),
        "order_prices re-derived key 1 from its live row"
    );
    assert_eq!(
        text_of(&client, "select total::text from order_totals where id = 1").await,
        Some("502".to_string()),
        "order_totals' row is unchanged"
    );
}

/// The whole-transform fuse (#799 rule 1) counts a definition's own held
/// keys: `order_totals` holding four keys and `order_prices` four more trips
/// neither, though the source has eight held keys, and `order_prices`' fifth
/// eviction trips it alone.
#[tokio::test]
async fn a_definitions_fuse_trips_on_its_own_evictions_only() {
    use trellis::staging::quarantine::DEFAULT_TRANSFORM_DEATH_THRESHOLD;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_two_readers(&db, &client).await;
    let orders = qualify_fixture_table("orders");
    let below = DEFAULT_TRANSFORM_DEATH_THRESHOLD - 1;

    for i in 0..below {
        insert_poison_marker(&client, "order_totals", &orders, &format!("held-{i}")).await;
    }
    for id in 11..11 + below {
        write_order(&client, id, "500", "1").await;
    }
    drain_through_evictions(&mut client, &db.pool).await;
    assert_eq!(
        rows_for(&client, "poison", "order_prices").await,
        below as i64
    );
    assert_eq!(transform_status(&client, "order_prices").await, "live");
    assert_eq!(transform_status(&client, "order_totals").await, "live");

    write_order(&client, 99, "500", "1").await;
    drain_through_evictions(&mut client, &db.pool).await;
    assert_eq!(
        transform_status(&client, "order_prices").await,
        "quarantined",
        "order_prices' own fifth held key trips its fuse"
    );
    assert_eq!(
        transform_status(&client, "order_totals").await,
        "live",
        "order_totals is charged for its own held keys only"
    );
    assert_eq!(
        text_of(
            &client,
            "select total::text from order_totals where id = 99"
        )
        .await,
        Some("501".to_string()),
        "order_totals applied every key order_prices failed on"
    );
}

/// A to-side key whose relationship work fails (#799): its image lacks the
/// relationship's `to_col`, which only the reverse work for the definitions
/// reading `par` through `parent` reads. Isolation finds the key fails with
/// `par`'s direct reader left out, so it holds the key for the relationship's
/// reader, `src_w`, whose share of the work then leaves it out, and `par_w`,
/// which reads `par` directly, applies it. Released, the key's `Recompute`
/// re-derives it from the live row, which has the column.
#[tokio::test]
async fn a_to_side_key_failing_in_its_relationship_work_is_held_for_its_relationship_readers() {
    const PAR: &str = "public.par";
    let mut d = drain_driver::Driver::start_with_relationships(
        "create table public.par (id integer primary key, code text unique, w numeric); \
         create table public.src (id integer primary key, p text); \
         insert into public.par values (1, 'a', 10); \
         insert into public.src values (1, 'a');",
        &[
            ("id", ValueType::Numeric),
            ("w", ValueType::Numeric),
            ("p", ValueType::Text),
        ],
        &["RELATIONSHIP parent FROM src.p TO par.code"],
        &[
            "TRANSFORM src_w FROM public.src SELECT parent.w AS pw",
            "TRANSFORM par_w FROM public.par SELECT w AS w",
        ],
        &[PAR, "public.src"],
    )
    .await;

    // Key 5's row, written past capture, and a change for it staged by hand
    // with an image that lacks `code`.
    d.ctl
        .batch_execute(
            "set session_replication_role = replica; \
             insert into public.par values (5, 'e', 9); \
             reset session_replication_role",
        )
        .await
        .expect("write par 5 uncaptured");
    let ring = active_ring_table(&d.ctl).await;
    insert_cdc_row(
        &d.ctl,
        &ring,
        PAR,
        "5",
        "insert",
        None,
        Some(r#"{"id":"5","w":"9"}"#),
    )
    .await;
    let seg_seq = d.seal().await;
    let mut failures = 0;
    while drain_result(d.pool(), seg_seq).await.is_err() {
        failures += 1;
        assert!(failures <= 20, "the page never committed");
    }
    assert_eq!(
        failures,
        trellis::staging::DEFAULT_DEATH_THRESHOLD as usize - 1
    );

    assert_eq!(
        poisoned_for(&d.ctl).await,
        vec![("src_w".to_string(), PAR.to_string(), "5".to_string())],
        "the key is held for the relationship's reader, not par's direct reader"
    );
    assert_eq!(
        text_of(&d.ctl, "select w::text from public.par_w where id = 5").await,
        Some("9".to_string()),
        "par_w applied the key"
    );
    assert_eq!(rows_for(&d.ctl, "poison_held", "src_w").await, 1);

    trellis::staging::release_key(d.pool(), "src_w", PAR, "5")
        .await
        .expect("release par 5 for src_w");
    let seg_seq = d.seal().await;
    assert!(
        drain_result(d.pool(), seg_seq).await.is_ok(),
        "the release's Recompute reads the live row, which has `code`"
    );
    assert_eq!(poisoned_for(&d.ctl).await, vec![]);
}

/// #799 with #663: a definition that holds a key and is then paused by a
/// halt is frozen, so the drain no longer reads its poison at all. A later
/// change to the key is neither applied nor parked for it (its resume
/// rebuilds it), and the sibling keeps applying the key. The resume clears
/// the halt and the held key together.
#[tokio::test]
async fn a_halted_definitions_held_key_is_neither_parked_again_nor_left_after_its_resume() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    hold_key_1_for_order_prices(&db, &mut client).await;
    let orders = qualify_fixture_table("orders");

    // Paused by a halt, as `staging::halt` pauses a closure.
    client
        .execute(
            "with d as ( \
                 update transform_definitions set status = 'paused' \
                 where split_part(target_table, '.', 2) = 'order_prices' returning id) \
             insert into capture_failures (transform_id, source_table, columns, error, kind) \
             select id, $1, '{}', 'the drain halted', 'halt' from d",
            &[&orders],
        )
        .await
        .expect("halt order_prices");

    write_order(&client, 1, "700", "4").await;
    assert_eq!(drain_through_evictions(&mut client, &db.pool).await, 0);
    assert_eq!(
        text_of(&client, "select total::text from order_totals where id = 1").await,
        Some("704".to_string()),
        "order_totals keeps applying key 1"
    );
    assert_eq!(
        rows_for(&client, "poison_held", "order_prices").await,
        1,
        "nothing more is parked for a frozen definition: its resume rebuilds it"
    );

    trellis::staging::quarantine::resume_transform(&db.pool, "order_prices")
        .await
        .expect("resume order_prices");
    for table in ["poison", "poison_held", "key_deaths", "capture_failures"] {
        assert_eq!(rows_for(&client, table, "order_prices").await, 0, "{table}");
    }
}

/// Review of #799: a to-side key whose relationship's every reader is frozen.
/// `src_w`, the only reader of `parent`, is paused, so `parent`'s reverse
/// work has no reader that isn't frozen, yet the drain still runs it, and
/// for key 5 it fails (the image lacks `code`). Isolation can charge only
/// `par_w`, the one direct reader. Once the key is held for `par_w`, the
/// change must be left out whole: `parent`'s work serves no reader that
/// isn't frozen (a resume of `src_w` refreshes its projection, #768), and a
/// failure in it that no definition can be charged for would otherwise fail
/// the page on every drain for good.
#[tokio::test]
async fn a_held_key_is_left_out_of_a_relationship_whose_every_reader_is_frozen() {
    const PAR: &str = "public.par";
    let mut d = drain_driver::Driver::start_with_relationships(
        "create table public.par (id integer primary key, code text unique, w numeric); \
         create table public.src (id integer primary key, p text); \
         insert into public.par values (1, 'a', 10); \
         insert into public.src values (1, 'a');",
        &[
            ("id", ValueType::Numeric),
            ("w", ValueType::Numeric),
            ("p", ValueType::Text),
        ],
        &["RELATIONSHIP parent FROM src.p TO par.code"],
        &[
            "TRANSFORM src_w FROM public.src SELECT parent.w AS pw",
            "TRANSFORM par_w FROM public.par SELECT w AS w",
        ],
        &[PAR, "public.src"],
    )
    .await;
    d.ctl
        .batch_execute(
            "update transform_definitions set status = 'paused' \
             where split_part(target_table, '.', 2) = 'src_w'",
        )
        .await
        .expect("pause src_w");

    d.ctl
        .batch_execute(
            "set session_replication_role = replica; \
             insert into public.par values (5, 'e', 9); \
             reset session_replication_role",
        )
        .await
        .expect("write par 5 uncaptured");
    let ring = active_ring_table(&d.ctl).await;
    insert_cdc_row(
        &d.ctl,
        &ring,
        PAR,
        "5",
        "insert",
        None,
        Some(r#"{"id":"5","w":"9"}"#),
    )
    .await;
    let seg_seq = d.seal().await;
    let mut failures = 0;
    while drain_result(d.pool(), seg_seq).await.is_err() {
        failures += 1;
        assert!(failures <= 20, "the page never committed");
    }
    assert_eq!(
        failures,
        trellis::staging::DEFAULT_DEATH_THRESHOLD as usize - 1
    );
    assert_eq!(
        poisoned_for(&d.ctl).await,
        vec![("par_w".to_string(), PAR.to_string(), "5".to_string())],
        "the only definition the drain could charge holds the key"
    );
}

/// Review of #799: a page parks a change for the definition its key was held
/// for when the page was computed, in its apply's own transaction. A release
/// (or a resume, rule 5) that lands in between deletes the key's `poison`
/// and `poison_held` rows, and re-derives the key from its live row. The page
/// must then park nothing: a held row for a key nothing holds is named by no
/// release, and blocks every watermark token from then on.
#[tokio::test]
async fn a_page_parks_nothing_for_a_key_released_after_it_was_computed() {
    use trellis::staging::{claim, fold};

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    hold_key_1_for_order_prices(&db, &mut client).await;
    let orders = qualify_fixture_table("orders");
    client
        .batch_execute("alter table public.order_prices drop constraint cheap")
        .await
        .expect("fix the cause");

    write_order(&client, 1, "60", "3").await;
    let seg_seq = seal_active_segment(&mut client).await;
    let mut phase1 = db.pool.get().await.expect("connection");
    let txn = phase1.transaction().await.expect("begin phase 1");
    claim::claim(&txn, seg_seq, "worker", 1)
        .await
        .expect("claim");
    let share = claim::held_share(&*txn, seg_seq, "worker")
        .await
        .expect("held_share");
    let folded = fold::fold(&txn, seg_seq, share.filter(share.buckets()))
        .await
        .expect("fold");
    txn.commit().await.expect("commit phase 1");
    let plan = apply::compute(&db.pool, &folded).await.expect("compute");

    // Released between the page's compute and its apply.
    trellis::staging::release_key(&db.pool, "order_prices", &orders, "1")
        .await
        .expect("release key 1 for order_prices");

    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg_seq,
        "worker",
        &plan,
        "trellis_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply the page");
    txn.commit().await.expect("commit phase 3");
    assert_eq!(
        rows_for(&client, "poison_held", "order_prices").await,
        0,
        "nothing is parked for a key no longer held"
    );

    // The release's Recompute re-derives the key, the page's change included.
    assert_eq!(drain_through_evictions(&mut client, &db.pool).await, 0);
    assert_eq!(
        text_of(&client, "select price::text from order_prices where id = 1").await,
        Some("60".to_string())
    );
    let token = converge::watermark_token(&client)
        .await
        .expect("watermark token");
    converge::await_converged(&client, token, std::time::Duration::from_secs(5))
        .await
        .expect("nothing is left holding the band");
}
