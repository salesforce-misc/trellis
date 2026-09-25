//! Integration tests for column-level quarantine
//! (`docs/decisions/0003-quarantine-storage-and-api.md`'s 2026-09-12
//! amendment, `docs/decisions/0008-public-api-design.md` decision 5): the per-`(transform,
//! column)` fuse layered alongside the pre-existing row-level/transform-wide
//! one (`trellis/tests/quarantine.rs`, unmodified by this feature — see
//! `an_existing_row_level_fuse_scenario_is_unaffected` below for a targeted
//! regression check of that claim in this file too).
//!
//! Follows `trellis/tests/quarantine.rs`'s own conventions: a real, ephemeral
//! Postgres instance per test (`testkit::TestCluster`), and "reach past the
//! mechanism, insert directly" for whichever half of a scenario the
//! mechanism under test doesn't itself produce (staging a malformed CDC
//! image directly into the ring, rather than routing through a live
//! replication slot).

use std::collections::HashMap;
use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{
    TransformStatus, chunk_queue, create_aggregate_target_table, create_definition,
    create_relationship, create_target_table, install_definition, relationship_projection,
    source_primary_key,
};
use trellis::staging::StagedWatermark;
use trellis::staging::apply::{self, ApplyError};
use trellis::staging::quarantine::{self, DEFAULT_COLUMN_DEATH_THRESHOLD};
use trellis::{BlockingTrellis, Config, Trellis, TrellisOptions};

// ---------------------------------------------------------------------
// Shared scaffolding (mirrors `tests/quarantine.rs`)
// ---------------------------------------------------------------------

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("set search_path to trellis, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'")
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

/// The ring table `insert_cdc_row` must target *right now* — `seg_0` is only
/// correct for a test's first, never-yet-sealed batch; every batch after
/// that has rotated the active ring slot forward (`seal_active_segment`
/// advances `segment_pointer`), and inserting into a already-sealed table
/// would silently stage into the wrong (already-closed) batch. Every test
/// below that stages more than one batch reads this fresh before each one.
async fn active_segment_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment_pointer")
        .get(0);
    match ring_slot {
        0 => "seg_0",
        1 => "seg_1",
        2 => "seg_2",
        3 => "seg_3",
        other => panic!("unexpected ring slot {other}"),
    }
    .to_string()
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// Bare (no `.`) `name` qualified under [`DEFAULT_SCHEMA`] — where every bare
/// `create table` in this file's own fixtures actually lands, since
/// `connect_raw` pins `search_path` to `trellis, public` and never qualifies
/// its own DDL. Matches `tests/quarantine.rs`'s (and `tests/apply.rs`'s) own
/// `qualify_fixture_table` (issue #74, ADR-0007): a CDC-staged `src_table`
/// must be fully qualified to match what a real CDC producer stages (issue
/// #76) and what the dependency graph now keys on (issue #74), or
/// `catalog::transforms_for_source` silently finds nothing and this whole
/// file's column-fuse machinery never even gets exercised. Already-qualified
/// input (containing a `.`) passes through unchanged.
fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
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
    let src_table = qualify_fixture_table(src_table);
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
        .unwrap_or_else(|e| panic!("insert cdc row {key:?} into {table} failed: {e}"));
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
/// target table — the single-column fixture most tests below trip the
/// column fuse against.
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

/// Adds a second, chained 1-1 transform (`order_summaries`, reading straight
/// from `order_totals`'s own `total` column) — the fixture the cascade tests
/// use. Mirrors `tests/quarantine.rs`'s `order_summary` hop-bound fixture.
async fn seed_order_summaries(db: &TestDatabase, orders_def: &TransformDef) {
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = create_definition(
        &db.pool,
        "TRANSFORM order_summaries FROM order_totals SELECT total + total AS grand_total",
        &order_totals_columns,
    )
    .await
    .expect("create order_summaries definition");
    let pk = source_primary_key(&db.pool, &orders_def.source)
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
    .expect("create order_summaries table");
}

/// Stages one malformed (`price` = `"not-a-number"`) CDC insert per id in
/// `ids`, all into the *same* segment, seals it, and drains it once —
/// expecting an evaluator failure to surface (the drive-by-real-failures
/// pattern `tests/quarantine.rs`'s own eviction test uses, just for the
/// column fuse instead of the row-level one).
///
/// Deliberately one segment per call, not one per `id`: none of these
/// batches ever actually reaches `drained` (a lone bad key's own
/// `key_deaths` never crosses the *row-level* fuse's threshold on its own,
/// so `drain_once` always gives up and propagates rather than evicting and
/// retrying to success) — the ring only has `RING_SIZE` (4) physical slots,
/// and nothing here ever retires a stuck segment, so batching every id this
/// call needs into one segment keeps every test's total segment count under
/// that ceiling instead of exhausting the ring.
async fn stage_bad_orders(client: &mut Client, pool: &trellis::Pool, ids: &[i64]) {
    let table = active_segment_table(client).await;
    for id in ids {
        insert_cdc_row(
            client,
            &table,
            "orders",
            &id.to_string(),
            "insert",
            None,
            Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
        )
        .await;
    }
    let seg_seq = seal_active_segment(client).await;
    let result = apply::drain_once(
        pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    assert!(
        matches!(result, Err(ApplyError::Eval(_))),
        "a malformed numeric field must still surface as an evaluator failure, got {result:?}"
    );
}

/// The persisted status of the transform whose bare target name is `target`.
async fn status_named(client: &Client, target: &str) -> String {
    client
        .query_one(
            "select status from transform_definitions \
             where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0)
}

async fn column_status_row(
    client: &Client,
    transform: &str,
    column: &str,
) -> Option<(bool, Option<String>)> {
    client
        .query_opt(
            "select local_fuse, last_error from column_status \
             where transform_table = $1 and column_name = $2",
            &[&transform, &column],
        )
        .await
        .expect("read column_status")
        .map(|row| (row.get(0), row.get(1)))
}

async fn column_deaths_count(client: &Client, transform: &str, column: &str) -> Option<i32> {
    client
        .query_opt(
            "select deaths from column_deaths where transform_table = $1 and column_name = $2",
            &[&transform, &column],
        )
        .await
        .expect("read column_deaths")
        .map(|row| row.get(0))
}

async fn cascade_edge_exists(
    client: &Client,
    downstream_transform: &str,
    downstream_column: &str,
    upstream_transform: &str,
    upstream_column: &str,
) -> bool {
    client
        .query_opt(
            "select 1 from column_pause_cascades \
             where downstream_transform = $1 and downstream_column = $2 \
               and upstream_transform = $3 and upstream_column = $4",
            &[
                &downstream_transform,
                &downstream_column,
                &upstream_transform,
                &upstream_column,
            ],
        )
        .await
        .expect("read column_pause_cascades")
        .is_some()
}

// ---------------------------------------------------------------------
// (a) The column fuse trips after the threshold is crossed, and not before.
// ---------------------------------------------------------------------

#[tokio::test]
async fn column_fuse_trips_only_once_the_threshold_is_crossed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    assert_eq!(
        DEFAULT_COLUMN_DEATH_THRESHOLD, 5,
        "test assumes the default"
    );

    // `DEFAULT_COLUMN_DEATH_THRESHOLD - 1` distinct bad rows, one batch:
    // charged, but not paused.
    let below_threshold: Vec<i64> = (1..DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &below_threshold).await;
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        Some(DEFAULT_COLUMN_DEATH_THRESHOLD - 1),
        "every distinct bad row below threshold must be charged exactly once"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "must not be paused before the threshold is reached"
    );

    // One more distinct bad row, its own batch: trips the fuse.
    stage_bad_orders(
        &mut client,
        &db.pool,
        &[DEFAULT_COLUMN_DEATH_THRESHOLD as i64],
    )
    .await;

    let status = column_status_row(&client, "order_totals", "total")
        .await
        .expect("the column must be paused now that the threshold is crossed");
    assert!(status.0, "a threshold trip is a local fuse, not a cascade");
    assert!(status.1.is_some(), "the tripping error must be recorded");
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        None,
        "the counter must reset once the fuse trips"
    );
}

// ---------------------------------------------------------------------
// (b) A paused column's value freezes across subsequent CDC deltas.
// ---------------------------------------------------------------------

#[tokio::test]
async fn paused_column_freezes_instead_of_going_null_or_being_overwritten() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    // A healthy row, computed successfully before anything pauses. The
    // source row exists too: Phase 3 checks each write against it (#344).
    client
        .execute(
            "insert into orders (id, price, tax) values (100, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("insert healthy source row");
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "100",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain the healthy row");
    let frozen_total: String = client
        .query_one("select total::text from order_totals where id = 100", &[])
        .await
        .expect("read order_totals")
        .get(0);
    assert_eq!(frozen_total, "11.50");

    // Trip the column fuse via `DEFAULT_COLUMN_DEATH_THRESHOLD` distinct bad
    // rows, none of which is id 100.
    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the fuse must have tripped"
    );

    // A brand-new, perfectly healthy delta to the *already-written* row: its
    // `total` must stay exactly as it was, not be recomputed, not go null.
    client
        .execute(
            "update orders set price = 999.00, tax = 999.00 where id = 100",
            &[],
        )
        .await
        .expect("update healthy source row");
    let table = active_segment_table(&client).await;
    insert_cdc_row(
        &client,
        &table,
        "orders",
        "100",
        "update",
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
        Some(r#"{"price":"999.00","tax":"999.00"}"#),
    )
    .await;
    let seg_seq2 = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq2,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain the update, minus the paused column");

    let total_after: Option<String> = client
        .query_one("select total::text from order_totals where id = 100", &[])
        .await
        .expect("read order_totals")
        .get(0);
    assert_eq!(
        total_after,
        Some("11.50".to_string()),
        "a paused column's value must freeze at its last successfully computed value, not go \
         null and not be overwritten by new (even valid) source data"
    );
}

// ---------------------------------------------------------------------
// (c) Cascading a paused column's pause to a dependent (chained) transform.
// ---------------------------------------------------------------------

#[tokio::test]
async fn a_dependent_transforms_column_cascades_to_paused_when_its_upstream_column_pauses() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;

    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;

    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the upstream column must have paused"
    );
    let downstream_status = column_status_row(&client, "order_summaries", "grand_total")
        .await
        .expect("the dependent column must have cascaded to paused");
    assert!(
        !downstream_status.0,
        "a purely cascaded pause is not this column's own local fuse"
    );
    assert!(
        cascade_edge_exists(
            &client,
            "order_summaries",
            "grand_total",
            "order_totals",
            "total"
        )
        .await,
        "the cascade edge must be recorded so resume can later un-cascade it correctly"
    );
}

// ---------------------------------------------------------------------
// (d) Resume clears the pause, recomputes, and respects an independent
//     reason a dependent has to stay paused.
// ---------------------------------------------------------------------

#[tokio::test]
async fn resume_recomputes_and_does_not_un_pause_a_dependent_with_its_own_reason() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;

    // Real, valid rows in the actual source table — the malformed CDC
    // images below are purely staged/synthetic (as in every other test in
    // this file), so resume's recompute (which reads the live table) finds
    // clean data once the fuse trips and is resumed.
    client
        .batch_execute(
            "insert into orders (id, price, tax) values \
             (1, 10.00, 1.00), (2, 20.00, 2.00), (3, 30.00, 3.00), \
             (4, 40.00, 4.00), (5, 50.00, 5.00)",
        )
        .await
        .expect("seed valid orders rows");

    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some()
    );
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some()
    );

    // Give the dependent its *own*, independent reason to stay paused —
    // reached past the mechanism directly, the same convention
    // `tests/quarantine.rs` uses for seeding counters/markers by hand.
    client
        .execute(
            "update column_status set local_fuse = true \
             where transform_table = 'order_summaries' and column_name = 'grand_total'",
            &[],
        )
        .await
        .expect("mark the dependent as also independently paused");

    // The batch that tripped the fuse rolled back entirely (it never
    // resolved), so `order_totals` has no rows for ids 1-5 yet — retry them
    // now that `total` is paused/excluded: this batch succeeds (nothing left
    // to error on) and creates the bare rows resume's recompute will then
    // fill in, exactly like a real drain loop retrying a previously-failing
    // batch once the column that broke it is out of the way.
    let table = active_segment_table(&client).await;
    for id in 1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64 {
        insert_cdc_row(
            &client,
            &table,
            "orders",
            &id.to_string(),
            "insert",
            None,
            Some(&format!(
                r#"{{"price":"{}.00","tax":"{}.00"}}"#,
                id * 10,
                id
            )),
        )
        .await;
    }
    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain now that the broken column is excluded");

    let resumed = quarantine::resume_column(&db.pool, "order_totals", "total")
        .await
        .expect("resume_column");
    assert_eq!(
        resumed,
        vec![("order_totals".to_string(), "total".to_string())],
        "the dependent must NOT be auto-resumed — it has its own independent reason"
    );

    // Issue #476: the resume's catch-up re-reads the source for changes the
    // recompute's single read missed, so until it has run the transform
    // reports `catching_up` (it keeps applying), and its discharge takes it
    // back to `live`.
    let status_of = async |client: &tokio_postgres::Client| -> String {
        client
            .query_one(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = 'order_totals'",
                &[],
            )
            .await
            .expect("read order_totals' status")
            .get(0)
    };
    assert_eq!(status_of(&client).await, "catching_up");
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("discharge the resume's catch-up");
    assert_eq!(status_of(&client).await, "live");

    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "the resumed column itself must no longer be paused"
    );
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some(),
        "the dependent must remain paused: its own local_fuse reason still holds"
    );
    assert!(
        !cascade_edge_exists(
            &client,
            "order_summaries",
            "grand_total",
            "order_totals",
            "total"
        )
        .await,
        "the specific cascade edge from the now-resumed upstream must be gone"
    );

    for id in 1..=5i32 {
        let total: String = client
            .query_one("select total::text from order_totals where id = $1", &[&id])
            .await
            .unwrap_or_else(|e| panic!("read order_totals for id {id}: {e}"))
            .get(0);
        let expected = match id {
            1 => "11.00",
            2 => "22.00",
            3 => "33.00",
            4 => "44.00",
            5 => "55.00",
            _ => unreachable!(),
        };
        assert_eq!(
            total, expected,
            "resume must have recomputed against the real (valid) source row for id {id}"
        );
    }
}

/// Reviewer follow-up to issue #74 (epic #78's own whole-branch review): the
/// quarantine-recompute counterpart to `apply.rs`'s
/// `explicitly_qualified_target_receives_a_live_cdc_write` and
/// `apply_aggregate.rs`'s
/// `explicitly_qualified_aggregate_target_receives_a_live_cdc_write` — a
/// definition installed with an explicit non-default *target* schema (issue
/// #76's grammar) used to fail `resume_column`'s recompute outright:
/// `recompute_column`'s own `UPDATE` (`staging::quarantine`) bound
/// `def.def.target` — always bare — straight into `quote_ident`, instead of
/// `Definition::target_table`'s qualified identity, so the write tried
/// bare, unqualified `order_totals`, not on this connection's pinned
/// `search_path`, even though `custom.order_totals` (the real target) does
/// exist and already carries the bare (non-`total`) columns a prior,
/// successful drain wrote for it.
#[tokio::test]
async fn resume_column_recomputes_an_explicitly_qualified_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create schema custom; \
             create table orders (id integer primary key, price numeric, tax numeric)",
        )
        .await
        .expect("seed source table and the custom schema");

    const DEF_TEXT: &str = "TRANSFORM custom.order_totals FROM orders SELECT price + tax AS total";
    let def = trellis::defs::parse(DEF_TEXT).expect("parse the explicitly-qualified target");
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "custom", &pk, &source_columns, &def.source)
        .await
        .expect("materialize custom.order_totals ahead of create_definition");
    create_definition(&db.pool, DEF_TEXT, &source_columns)
        .await
        .expect("create definition against the explicitly-qualified target");

    // Trip the column fuse exactly like this file's other tests — the
    // malformed images below are purely staged/synthetic and never touch
    // the physical `orders` table.
    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the fuse must have tripped"
    );

    // A real, valid row in the physical source table, plus a bare (`total`
    // excluded) target row for it — standing in for what a drain retried
    // after the column paused would have already written, the state
    // `resume_column`'s recompute is meant to fill in.
    client
        .execute(
            "insert into orders (id, price, tax) values (200, 10.00, 5.00)",
            &[],
        )
        .await
        .expect("seed a valid source row");
    client
        .execute("insert into custom.order_totals (id) values (200)", &[])
        .await
        .expect("pre-populate a bare target row for the row resume must fill in");

    let resumed = quarantine::resume_column(&db.pool, "order_totals", "total")
        .await
        .expect(
            "resume_column must recompute against custom.order_totals — a bare, \
             unqualified UPDATE would have raised relation \"order_totals\" does \
             not exist instead",
        );
    assert_eq!(
        resumed,
        vec![("order_totals".to_string(), "total".to_string())]
    );

    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "the column must no longer be paused after a successful resume"
    );

    let total: String = client
        .query_one(
            "select total::text from custom.order_totals where id = 200",
            &[],
        )
        .await
        .expect("read custom.order_totals")
        .get(0);
    assert_eq!(
        total, "15.00",
        "resume must have recomputed custom.order_totals against the real source row"
    );

    let bare_decoy_exists: bool = client
        .query_one(
            "select exists (select 1 from information_schema.tables \
             where table_name = 'order_totals' and table_schema <> 'custom')",
            &[],
        )
        .await
        .expect("check for a same-named decoy outside the custom schema")
        .get(0);
    assert!(
        !bare_decoy_exists,
        "the recompute must land in custom.order_totals, never create/touch a \
         same-named table in some other schema on the connection's search_path"
    );
}

// ---------------------------------------------------------------------
// (e) The three read methods and the resume method, on both `Trellis` and
//     `BlockingTrellis`.
// ---------------------------------------------------------------------

#[tokio::test]
async fn trellis_exposes_the_read_and_resume_methods() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    let list = trellis.quarantined().await.expect("quarantined");
    assert!(
        list.iter().any(|entry| entry.target
            == trellis::app::QuarantineTarget::Column(
                "order_totals".to_string(),
                "total".to_string()
            )),
        "the paused column must appear in the flat list: {list:?}"
    );

    let status = trellis
        .quarantine_status("order_totals.total")
        .await
        .expect("quarantine_status");
    assert_eq!(
        status.state,
        trellis::app::QuarantineState::Paused,
        "the column address must report paused"
    );
    assert!(status.paused_at.is_some());

    let whole_transform_status = trellis
        .quarantine_status("order_totals")
        .await
        .expect("quarantine_status for the bare transform");
    assert_eq!(
        whole_transform_status.state,
        trellis::app::QuarantineState::Live,
        "the transform's own lifecycle status is untouched by a column pause"
    );

    let sample = trellis
        .sample_quarantined("order_totals.total", None, 10)
        .await
        .expect("sample_quarantined");
    assert_eq!(
        sample.len(),
        DEFAULT_COLUMN_DEATH_THRESHOLD as usize,
        "every distinct poisoned row that contributed to the trip must be sampleable: {sample:?}"
    );

    let resumed = trellis
        .apply("RESUME TRANSFORM order_totals.total")
        .await
        .expect("resume a paused column");
    assert!(
        matches!(&resumed, trellis::Applied::Resumed { columns }
            if columns == &vec![("order_totals".to_string(), "total".to_string())]),
        "a column resume reports the pairs it resumed, got {resumed:?}"
    );

    let status_after = trellis
        .quarantine_status("order_totals.total")
        .await
        .expect("quarantine_status after resume");
    assert_eq!(status_after.state, trellis::app::QuarantineState::Live);

    // Issue #227 replaced `resume_column`'s "a bare address is caller error"
    // rejection with a grammar that makes the two spellings *different
    // statements*: `RESUME TRANSFORM <t>.<c>` resumes one column (above),
    // while `RESUME TRANSFORM <t>` is the whole-transform rebuild. So the bare
    // form is no longer an addressing error at all — it is the other
    // operation, judged against that operation's own precondition, which this
    // still-live transform doesn't meet (only a frozen definition can be
    // resumed).
    let bare = trellis.apply("RESUME TRANSFORM order_totals").await;
    assert!(
        matches!(
            bare,
            Err(trellis::TrellisError::Apply(
                ApplyError::TransformNotPaused { .. }
            ))
        ),
        "a bare address is the whole-transform resume, refused here because the transform \
         itself is live — not an ambiguous-address error, got {bare:?}"
    );
}

#[test]
fn blocking_trellis_exposes_the_read_and_resume_methods() {
    let cluster = TestCluster::start();

    let setup_runtime = tokio::runtime::Runtime::new().expect("build scratch setup runtime");
    let db = setup_runtime.block_on(cluster.create_isolated_database());
    setup_runtime.block_on(async {
        let mut client = connect_raw(db.dsn()).await;
        seed_order_totals(&db, &client).await;
        let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
        stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    });
    drop(setup_runtime);

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis =
        BlockingTrellis::connect(config, TrellisOptions::default()).expect("connect (sync)");

    let list = trellis.quarantined().expect("quarantined (sync)");
    assert!(
        list.iter().any(|entry| entry.target
            == trellis::app::QuarantineTarget::Column(
                "order_totals".to_string(),
                "total".to_string()
            )),
        "the paused column must appear in the flat list: {list:?}"
    );

    let status = trellis
        .quarantine_status("order_totals.total")
        .expect("quarantine_status (sync)");
    assert_eq!(status.state, trellis::app::QuarantineState::Paused);

    let sample = trellis
        .sample_quarantined("order_totals.total", None, 10)
        .expect("sample_quarantined (sync)");
    assert_eq!(sample.len(), DEFAULT_COLUMN_DEATH_THRESHOLD as usize);

    let resumed = trellis
        .apply("RESUME TRANSFORM order_totals.total")
        .expect("resume a paused column (sync)");
    assert!(
        matches!(&resumed, trellis::Applied::Resumed { columns }
            if columns == &vec![("order_totals".to_string(), "total".to_string())]),
        "a column resume reports the pairs it resumed, got {resumed:?}"
    );

    let status_after = trellis
        .quarantine_status("order_totals.total")
        .expect("quarantine_status after resume (sync)");
    assert_eq!(status_after.state, trellis::app::QuarantineState::Live);

    trellis.shutdown().expect("shutdown (sync)");
}

// ---------------------------------------------------------------------
// (f) The existing row-level/transform-wide fuse still works unchanged.
// ---------------------------------------------------------------------

/// A targeted regression check living alongside the new feature's own
/// tests, on top of `trellis/tests/quarantine.rs`'s full existing suite
/// (unmodified, still green): a single stubborn key retried past the
/// row-level threshold must still evict via `key_deaths`/`poison` exactly as
/// before, and — the specific new-feature interaction this test is really
/// about — must NOT also trip the column fuse, since it is one row failing
/// repeatedly, not a breadth of distinct rows (see `column_failures`'
/// migration comment / `charge_column_failure`'s doc comment on why the
/// column fuse counts distinct rows, not attempts).
#[tokio::test]
async fn an_existing_row_level_fuse_scenario_is_unaffected() {
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

    let outcome = loop {
        match apply::drain_once(
            &db.pool,
            seg_seq,
            "worker",
            1,
            "trellis_column_quarantine_test",
            &StagedWatermark::saturated(),
        )
        .await
        {
            Ok(Some(outcome)) => break outcome,
            Ok(None) => panic!("drain_once claimed nothing on a still-undrained segment"),
            Err(_) => continue,
        }
    };
    assert_eq!(outcome.keys_written, 1, "only the survivor, key 2, writes");

    let orders = qualify_fixture_table("orders");
    let poisoned: bool = client
        .query_one(
            "select exists(select 1 from poison where src_table = $1 and key = '1')",
            &[&orders],
        )
        .await
        .expect("read poison")
        .get(0);
    assert!(poisoned, "the row-level fuse must still evict as before");

    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        Some(1),
        "one repeatedly-retried key must charge the column counter exactly once, not once per \
         attempt — it never crosses the column fuse's own threshold alone"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "a single stubborn row must never trip the column fuse on its own"
    );
}

// ---------------------------------------------------------------------
// (g) Regression: a re-executed durable backfill chunk must leave a paused
//     column untouched (must-fix 1).
// ---------------------------------------------------------------------

/// A durable backfill chunk (`defs::chunk_queue`, ADR-0007's amendment) is
/// claimable and crash-recoverable: a chunk already marked `done` can still
/// be re-executed (e.g. after a reclaim following a crash — see
/// `trellis/tests/defs_backfill_chunk_queue.rs`'s own reclaim-and-redo
/// tests), and `defs::backfill::write_one_to_one_range`/
/// `execute_one_to_one_chunk` had zero awareness of `column_status`: a
/// re-executed chunk's `ON CONFLICT DO UPDATE` blindly overwrote *every*
/// field, including one live CDC had since paused, silently undoing the
/// freeze. This drives the chunk-queue execution path directly (as the task
/// suggests, in lieu of orchestrating a real crash) to prove the fix: a
/// paused column's value must survive a re-executed chunk write untouched,
/// while a sibling, non-paused column in the very same row must still pick
/// up the re-executed chunk's freshly computed value.
#[tokio::test]
async fn a_reexecuted_backfill_chunk_leaves_a_paused_column_untouched() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric, b numeric); \
             insert into s (id, a, b) values (1, 10, 100), (2, 20, 200)",
        )
        .await
        .expect("seed source");

    let cols = numeric_columns(&["a", "b"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x, b + b AS y",
        &cols,
        "public",
    )
    .await
    .expect("install_definition records the definition and returns");
    assert_eq!(
        def.status,
        TransformStatus::WaitingToBackfill,
        "registration only records the definition (ADR-0016)"
    );
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("the discharge dispatches the chunked build");
    assert_eq!(
        status_named(&client, def.def.target.as_str()).await,
        "backfilling",
        "a plain 1-1 definition sits at backfilling until its chunk is claimed and finished"
    );

    let claimed = chunk_queue::claim_chunks(&client, "worker-1", 10)
        .await
        .expect("claim_chunks");
    assert_eq!(claimed.len(), 1, "one chunk covers this small source table");

    chunk_queue::run_claimed_chunk(&db.pool, &claimed[0], "worker-1", Duration::from_secs(5))
        .await
        .expect("run_claimed_chunk (initial build)");
    chunk_queue::finish_chunk(&db.pool, &claimed[0], "worker-1")
        .await
        .expect("finish_chunk");

    let initial = client
        .query_one("select x::text, y::text from t where id = 1", &[])
        .await
        .expect("read t after initial build");
    let (x_initial, y_initial): (String, String) = (initial.get(0), initial.get(1));
    assert_eq!(x_initial, "20");
    assert_eq!(y_initial, "200");

    // Simulate live CDC having since paused column `x` — reached past the
    // mechanism, inserted directly, the same convention the rest of this
    // file uses for whichever half of a scenario isn't the mechanism under
    // test (here, *how* the pause happened is irrelevant; only its effect
    // on a re-executed chunk is).
    client
        .execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('t', 'x', 'synthetic pause for backfill freeze test', true)",
            &[],
        )
        .await
        .expect("seed column_status directly");

    // Mutate the source row so a naive re-execution would compute different
    // values for *both* columns — `x` must stay frozen; `y` must not.
    client
        .execute("update s set a = 999, b = 500 where id = 1", &[])
        .await
        .expect("mutate source row 1");

    // Simulate the chunk being reclaimed (e.g. after a crash) and
    // re-executed by a different worker — driving the chunk-queue execution
    // path directly, exactly as it would be after
    // `chunk_queue::reclaim_stale_chunks` frees a dead claim.
    chunk_queue::run_claimed_chunk(&db.pool, &claimed[0], "worker-2", Duration::from_secs(5))
        .await
        .expect("run_claimed_chunk (re-executed after simulated reclaim)");

    let after = client
        .query_one("select x::text, y::text from t where id = 1", &[])
        .await
        .expect("read t after re-executed chunk");
    let (x_after, y_after): (String, String) = (after.get(0), after.get(1));
    assert_eq!(
        x_after, x_initial,
        "a paused column's value must be untouched by a re-executed backfill chunk"
    );
    assert_eq!(
        y_after, "1000",
        "a non-paused column in the very same row must still be updated by the re-executed chunk"
    );
}

// ---------------------------------------------------------------------
// (h) Regression: cascade must never reach a KeySpace::Aggregate transform
//     (must-fix 2).
// ---------------------------------------------------------------------

/// `column_dependents` used to scan every `transform_definitions` row with
/// no `KeySpace` filter, so a downstream aggregate transform whose field
/// happens to read a just-paused upstream 1-1 column would get a wrongly
/// cascaded `column_status` row — one `staging::apply_aggregate`'s
/// incremental-delta path has no notion of and would never clear, and one
/// `resume_column`'s cascade walk would later mishandle via the 1-1-only
/// `recompute_column`. Proves the fix: pausing the upstream column must
/// never create a `column_status` row for the aggregate, and the aggregate's
/// own write path must keep functioning normally afterward.
#[tokio::test]
async fn pausing_an_upstream_column_never_cascades_into_a_downstream_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    client
        .batch_execute("alter table order_totals replica identity full")
        .await
        .expect("set replica identity full (an aggregate source needs full pre-images)");

    // A downstream aggregate transform reading `order_totals.total` — the
    // scope-cut this fix enforces: column-level pause/cascade/resume never
    // touches an aggregate transform.
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let stats_def = create_definition(
        &db.pool,
        "TRANSFORM order_stats FROM order_totals GROUP BY id SELECT id AS id, \
         SUM(total) AS total_sum",
        &order_totals_columns,
    )
    .await
    .expect("create order_stats definition");
    create_aggregate_target_table(&db.pool, &stats_def.def, "public", &order_totals_columns)
        .await
        .expect("create order_stats table");

    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;

    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the upstream column must have paused"
    );
    assert_eq!(
        column_status_row(&client, "order_stats", "total_sum").await,
        None,
        "an aggregate transform must never get a column_status row via cascade — aggregates are \
         out of scope for column-level pause/cascade/resume"
    );

    // A subsequent, healthy batch flowing all the way through to the
    // aggregate: the write path must still complete normally (no error, no
    // wedge) — proving the (correctly withheld) cascade never left the
    // aggregate's own status or write path in a broken state.
    client
        .batch_execute("insert into orders (id, price, tax) values (999, 5.00, 1.00)")
        .await
        .expect("seed a healthy order row");
    let table = active_segment_table(&client).await;
    insert_cdc_row(
        &client,
        &table,
        "orders",
        "999",
        "insert",
        None,
        Some(r#"{"price":"5.00","tax":"1.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("a healthy batch must still drain normally through order_totals");

    // The write to `order_totals` above stages a downstream `Recompute`
    // marker for `order_stats` into the (still-open) active ring segment —
    // seal and drain once more to actually run the aggregate's own write
    // path against it.
    let seg_seq2 = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq2,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect(
        "the aggregate's own batch must still drain normally, unaffected by the upstream \
             (correctly non-cascaded) pause",
    );

    let stats_row = client
        .query_opt(
            "select total_sum::text from order_stats where id = 999",
            &[],
        )
        .await
        .expect("read order_stats");
    assert!(
        stats_row.is_some(),
        "the aggregate transform must still write a row normally, unaffected by the upstream \
         (correctly non-cascaded) pause"
    );
}

// ---------------------------------------------------------------------
// (i) Regression: resuming a column must not silently no-op because a
//     sibling column on the same transform is also paused (must-fix 3).
// ---------------------------------------------------------------------

/// `resume_column`'s `recompute_column` used to call the un-excluding
/// `eval::evaluate_with_relationships`, so a still-paused sibling column
/// (`busted` below, whose formula throws for every row given the malformed —
/// but genuinely persisted — source data) made the *whole* per-row
/// evaluation fail, and the resumed column (`doubled`) silently never got
/// recomputed for any row, even though its own formula is perfectly healthy.
/// Proves the fix: excluding the still-paused sibling from evaluation lets
/// the resumed column recompute correctly for every row regardless.
#[tokio::test]
async fn resume_recomputes_correctly_even_when_a_sibling_column_still_throws() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // `tax` is a real Postgres `text` column holding a permanently
    // non-numeric value — `busted`'s formula (declared `Numeric` via
    // `source_columns`, same "reach past the mechanism" trick the rest of
    // this file uses for a malformed CDC image, just baked into real,
    // persisted source data here since `recompute_column` reads the live
    // table directly rather than a staged image) throws on every row, for
    // every recompute attempt, indefinitely.
    client
        .batch_execute(
            "create table calc_src (id integer primary key, price numeric, tax text); \
             insert into calc_src (id, price, tax) values \
             (1, 10, 'not-a-number'), (2, 20, 'not-a-number'), (3, 30, 'not-a-number')",
        )
        .await
        .expect("seed source table");

    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM calc FROM calc_src SELECT price + price AS doubled, tax + tax AS busted",
        &source_columns,
    )
    .await
    .expect("create calc definition");
    let calc_def = TransformDef {
        target: "calc".to_string(),
        source: "calc_src".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            FieldDef {
                name: "doubled".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("price".to_string())),
                    rhs: Box::new(Expr::Column("price".to_string())),
                },
            },
            FieldDef {
                name: "busted".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("tax".to_string())),
                    rhs: Box::new(Expr::Column("tax".to_string())),
                },
            },
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let pk = source_primary_key(&db.pool, "calc_src")
        .await
        .expect("introspect source primary key");
    create_target_table(
        &db.pool,
        &calc_def,
        "public",
        &pk,
        &source_columns,
        &calc_def.source,
    )
    .await
    .expect("create calc table");

    // Seed the target with sentinel values distinct from any real
    // recomputed result, so a successful recompute is unambiguous.
    client
        .batch_execute(
            "insert into calc (id, doubled, busted) values \
             (1, -999, -999), (2, -999, -999), (3, -999, -999)",
        )
        .await
        .expect("seed sentinel target rows");

    // Both columns independently paused — reached past the mechanism,
    // inserted directly (same convention as this file's other tests).
    client
        .batch_execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('calc', 'doubled', 'synthetic pause A', true), \
                    ('calc', 'busted', 'synthetic pause B (always throws)', true)",
        )
        .await
        .expect("seed column_status for both columns");

    let resumed = quarantine::resume_column(&db.pool, "calc", "doubled")
        .await
        .expect("resume_column");
    assert_eq!(
        resumed,
        vec![("calc".to_string(), "doubled".to_string())],
        "only the resumed column itself, nothing cascaded"
    );

    assert_eq!(
        column_status_row(&client, "calc", "doubled").await,
        None,
        "the resumed column must no longer be paused"
    );
    assert!(
        column_status_row(&client, "calc", "busted").await.is_some(),
        "the still-broken sibling must remain paused — resuming `doubled` must not touch it"
    );

    for (id, expected_doubled) in [(1, "20"), (2, "40"), (3, "60")] {
        let row = client
            .query_one(
                "select doubled::text, busted::text from calc where id = $1",
                &[&id],
            )
            .await
            .unwrap_or_else(|e| panic!("read calc for id {id}: {e}"));
        let doubled: String = row.get(0);
        let busted: String = row.get(1);
        assert_eq!(
            doubled, expected_doubled,
            "the resumed column must be correctly recomputed for id {id}, even though the \
             still-paused sibling's formula throws on every row"
        );
        assert_eq!(
            busted, "-999",
            "the still-paused sibling's own value must be left untouched by this resume"
        );
    }
}

/// Issue #377: `recompute_column`'s write-back matches each chunk of keys
/// against the target's own primary-key columns (an indexed keyset join)
/// rather than against the computed key-contract text. Resuming over a
/// composite key, across more than one write-back chunk, with key parts that
/// hold the key contract's own separator and escape characters, must still
/// land every recomputed value on exactly its own row, and must not invent a
/// row for a source key the target doesn't have.
#[tokio::test]
async fn resume_recomputes_every_row_of_a_composite_key_target_across_chunks() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table comp_src (region text, id integer, price numeric, \
                                    primary key (region, id)); \
             insert into comp_src (region, id, price) \
             select 'r' || (g % 3), g, g from generate_series(1, 2500) g; \
             insert into comp_src (region, id, price) values \
             ('a' || chr(31) || 'b', 1, 7), ('c' || chr(30) || 'd', 1, 8)",
        )
        .await
        .expect("seed source table");

    let source_columns = numeric_columns(&["id", "price"]);
    create_definition(
        &db.pool,
        "TRANSFORM comp FROM comp_src SELECT price + price AS doubled",
        &source_columns,
    )
    .await
    .expect("create comp definition");
    let comp_def = TransformDef {
        target: "comp".to_string(),
        source: "comp_src".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "doubled".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::Column("price".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let pk = source_primary_key(&db.pool, "comp_src")
        .await
        .expect("introspect source primary key");
    assert_eq!(pk.len(), 2, "the fixture must exercise a composite key");
    create_target_table(
        &db.pool,
        &comp_def,
        "public",
        &pk,
        &source_columns,
        &comp_def.source,
    )
    .await
    .expect("create comp table");

    // Sentinel values for every source row but one, which the target
    // deliberately lacks.
    client
        .batch_execute(
            "insert into comp (region, id, doubled) \
             select region, id, -999 from comp_src where not (region = 'r1' and id = 7); \
             insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('comp', 'doubled', 'synthetic pause', true)",
        )
        .await
        .expect("seed sentinel target rows and the pause");

    quarantine::resume_column(&db.pool, "comp", "doubled")
        .await
        .expect("resume_column");

    let row = client
        .query_one(
            "select count(*), \
                    count(*) filter (where t.doubled is distinct from s.price * 2), \
                    count(*) filter (where s.id is null) \
             from comp t left join comp_src s using (region, id)",
            &[],
        )
        .await
        .expect("compare target to source");
    let (total, wrong, orphans): (i64, i64, i64) = (row.get(0), row.get(1), row.get(2));
    assert_eq!(total, 2501, "no row may be added for the missing key");
    assert_eq!(
        wrong, 0,
        "every existing row must hold its own recomputed value"
    );
    assert_eq!(orphans, 0);
}

// ---------------------------------------------------------------------
// (j) Regression: ambiguous field-name attribution must fall back to no
//     column-level attribution (should-fix 4).
// ---------------------------------------------------------------------

/// Two sibling `KeySpace::OneToOne` transforms on the same source table,
/// both declaring a field named `total` — only `sib_b`'s formula is
/// actually broken (it reads `qty`, staged as unparseable below; `sib_a`
/// only reads `price`/`tax`, both fine). `attribute_column_failure` used to
/// resolve the ambiguity by picking whichever candidate
/// `transforms_for_source` happened to return first (lowest id), a
/// deterministic misattribution: it could freeze `sib_a`'s perfectly
/// healthy `total` while `sib_b`'s actually-broken one never accumulates a
/// `column_status` entry at all. Proves the fix: neither sibling gets a
/// column-level attribution, while the pre-existing row-level/transform-wide
/// fuse still evicts the stubborn key exactly as it did before this
/// feature.
#[tokio::test]
async fn ambiguous_field_name_attribution_falls_back_to_no_column_level_attribution() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table shared_src (id integer primary key, price numeric, tax numeric, \
             qty numeric)",
        )
        .await
        .expect("seed source table");
    let source_columns = numeric_columns(&["id", "price", "tax", "qty"]);

    create_definition(
        &db.pool,
        "TRANSFORM sib_a FROM shared_src SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create sib_a definition");
    create_definition(
        &db.pool,
        "TRANSFORM sib_b FROM shared_src SELECT qty + qty AS total",
        &source_columns,
    )
    .await
    .expect("create sib_b definition");

    let pk = source_primary_key(&db.pool, "shared_src")
        .await
        .expect("introspect source primary key");
    let def_a = TransformDef {
        target: "sib_a".to_string(),
        source: "shared_src".to_string(),
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
    };
    create_target_table(
        &db.pool,
        &def_a,
        "public",
        &pk,
        &source_columns,
        &def_a.source,
    )
    .await
    .expect("create sib_a table");
    let def_b = TransformDef {
        target: "sib_b".to_string(),
        source: "shared_src".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("qty".to_string())),
                rhs: Box::new(Expr::Column("qty".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    create_target_table(
        &db.pool,
        &def_b,
        "public",
        &pk,
        &source_columns,
        &def_b.source,
    )
    .await
    .expect("create sib_b table");

    // `DEFAULT_COLUMN_DEATH_THRESHOLD` distinct bad rows, all in one batch —
    // the same "breadth of distinct rows, not one row retried" shape
    // `stage_bad_orders`/`column_fuse_trips_only_once_the_threshold_is_crossed`
    // use elsewhere in this file, chosen deliberately here too: it's enough
    // volume to actually cross a column fuse's threshold, so this test would
    // catch the old deterministic-misattribution bug (which would have
    // tripped `sib_a`'s fuse — the healthy sibling, since it's the
    // lower-`id` candidate `transforms_for_source` returns first) rather
    // than vacuously passing because nothing ever reached threshold.
    let table = active_segment_table(&client).await;
    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    for id in &bad_ids {
        insert_cdc_row(
            &client,
            &table,
            "shared_src",
            &id.to_string(),
            "insert",
            None,
            Some(r#"{"price":"10.00","tax":"1.50","qty":"not-a-number"}"#),
        )
        .await;
    }
    let seg_seq = seal_active_segment(&mut client).await;
    let result = apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    assert!(
        matches!(result, Err(ApplyError::Eval(_))),
        "the malformed qty must still surface as an evaluator failure, got {result:?}"
    );

    assert_eq!(
        column_status_row(&client, "sib_a", "total").await,
        None,
        "the healthy sibling must never be attributed to, even though its field name matches \
         the actually-broken sibling's"
    );
    assert_eq!(
        column_status_row(&client, "sib_b", "total").await,
        None,
        "the actually-broken sibling must also get no column-level attribution — ambiguous \
         attribution must fall back to no attribution at all, not a guess"
    );

    // The pre-existing row-level fuse's own bookkeeping must be completely
    // unaffected by the ambiguity fallback: every distinct bad row is still
    // charged toward its own key-level counter exactly as it would be
    // without this feature (`tests/quarantine.rs`'s own fuse, untouched).
    let shared_src = qualify_fixture_table("shared_src");
    for id in &bad_ids {
        let deaths: Option<i32> = client
            .query_opt(
                "select deaths from key_deaths where src_table = $1 and key = $2",
                &[&shared_src, &id.to_string()],
            )
            .await
            .expect("read key_deaths")
            .map(|row| row.get(0));
        assert_eq!(
            deaths,
            Some(1),
            "row-level key_deaths bookkeeping for id {id} must proceed normally, unaffected by \
             the ambiguity fallback"
        );
    }
}

// ---------------------------------------------------------------------
// (k) Regression: `resume_column` must refuse a column whose definition
//     isn't live yet (the mid-backfill cascade bug a final holistic review
//     agent found).
// ---------------------------------------------------------------------

/// A pause reaching `(transform, column)` while `transform` is still
/// `Backfilling` — most concretely via this branch's cascade pause
/// (`defs::catalog::column_dependents`, unlike the `status = 'live'`
/// filtered lookups ordinary CDC apply uses, does *not* require the
/// downstream dependent to be live before cascading a pause onto it) —
/// must not be resumable. `recompute_column` takes exactly one snapshot of
/// the *source* table and writes back only via `update ... where pk = $2`
/// (no `insert`/upsert fallback); `column_status` for the paused column
/// stays present for the entire duration of that recompute and is only
/// deleted afterward. If the target definition is still mid-backfill, its
/// own `backfill_chunks` queue can be actively inserting brand-new rows
/// into the target the whole time `paused_columns_for` (backfill) and
/// `compute` (live CDC apply) are excluding this column from — a row
/// inserted after `recompute_column`'s snapshot was taken is never in its
/// `rows_by_pk` map and is never revisited once `column_status` is cleared,
/// permanently stranding that row's column even though `resume_column`
/// reports success. Proves the fix instead: `resume_column` returns
/// [`ApplyError::DefinitionNotLive`] and leaves everything untouched.
#[tokio::test]
async fn resume_column_refuses_a_column_on_a_not_yet_live_definition() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table stuck_src (id bigint primary key, a numeric); \
             insert into stuck_src (id, a) values (1, 10), (2, 20), (3, 30)",
        )
        .await
        .expect("seed source table");

    let cols = numeric_columns(&["a"]);
    // A plain 1-1 definition's discharge persists it as `Backfilling` and
    // enqueues its build as a `backfill_chunks` row — nothing in this test
    // ever claims/runs/finishes that chunk, so the definition is genuinely,
    // deterministically stuck in `Backfilling` for the rest of the test.
    // Same "nothing is watching the queue" determinism
    // `trellis/tests/defs_backfill_chunk_queue.rs`'s
    // `a_chunk_abandoned_by_its_claimant_is_reclaimed_and_completed_by_another_worker`
    // and `trellis/tests/blocking_trellis.rs`'s
    // `define_returns_before_backfill_completes` both rely on.
    let def = install_definition(
        &db.pool,
        "TRANSFORM stuck FROM stuck_src SELECT a + a AS doubled",
        &cols,
        "public",
    )
    .await
    .expect("install_definition records the definition and returns");
    assert_eq!(def.status, TransformStatus::WaitingToBackfill);
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("the discharge dispatches the chunked build");
    assert_eq!(
        status_named(&client, def.def.target.as_str()).await,
        "backfilling",
        "nothing drains the chunk queue in this test, so the definition must still be \
         backfilling"
    );

    // Simulate a pause landing on `stuck.doubled` while it's mid-backfill —
    // reached past the mechanism, inserted directly (this file's own
    // convention throughout; a real cascade would populate the same rows
    // via `cascade_pause`). Seed `column_deaths` too, so this test can also
    // prove the gate is all-or-nothing rather than partially cleaning up.
    client
        .batch_execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('stuck', 'doubled', 'paused because upstream column X is paused', false); \
             insert into column_deaths (transform_table, column_name, deaths) \
             values ('stuck', 'doubled', 3)",
        )
        .await
        .expect("seed column_status/column_deaths for the cascaded pause");

    let result = quarantine::resume_column(&db.pool, "stuck", "doubled").await;
    assert!(
        matches!(
            &result,
            Err(ApplyError::DefinitionNotLive { transform }) if transform == "stuck"
        ),
        "resuming a column on a not-yet-live definition must be refused, got {result:?}"
    );

    assert_eq!(
        column_status_row(&client, "stuck", "doubled").await,
        Some((
            false,
            Some("paused because upstream column X is paused".to_string())
        )),
        "a refused resume must leave column_status completely untouched"
    );
    assert_eq!(
        column_deaths_count(&client, "stuck", "doubled").await,
        Some(3),
        "a refused resume must be all-or-nothing: column_deaths must not be cleared either"
    );
}

/// The end-to-end version of the same bug, through the *real*
/// `trip_column_fuse` -> `cascade_pause` path rather than a hand-seeded
/// `column_status` row: `order_totals.total` (live) pauses and cascades onto
/// `order_summaries.grand_total`, whose definition is genuinely stuck
/// `Backfilling` behind its own never-drained `backfill_chunks` queue
/// (`install_definition`, same determinism as the test above). Proves the
/// cascade-queue's per-pair check (the second gate inside `resume_column`'s
/// `while` loop, distinct from the initial-pair gate the test above
/// exercises): resuming the upstream column must still succeed and commit,
/// while the downstream pair it cascaded onto is left exactly as it was —
/// still paused, not resumed, and the call does not error out just because
/// one pair deep in the queue isn't live yet.
#[tokio::test]
async fn resume_column_leaves_a_cascaded_not_yet_live_dependent_paused_without_erroring() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    // Real, valid source rows, drained so `order_totals` has actual target
    // rows for `install_definition` (below) to enumerate chunk work over —
    // an empty source table would enqueue zero chunks and `order_summaries`
    // would complete its (trivial) backfill synchronously instead of
    // sticking in `Backfilling` the way this test needs.
    client
        .batch_execute(
            "insert into orders (id, price, tax) values \
             (1, 10.00, 1.00), (2, 20.00, 2.00), (3, 30.00, 3.00)",
        )
        .await
        .expect("seed valid orders rows");
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.00"}"#),
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
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "3",
        "insert",
        None,
        Some(r#"{"price":"30.00","tax":"3.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain the seed rows into order_totals");

    // `order_summaries`, reading straight from `order_totals` (mirrors
    // `seed_order_summaries`), but installed via `install_definition` so it
    // enumerates real chunk work and is left `Backfilling` — nothing here
    // ever drains that queue, the same determinism
    // `resume_column_refuses_a_column_on_a_not_yet_live_definition` (above)
    // and `trellis/tests/defs_backfill_chunk_queue.rs` rely on.
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = install_definition(
        &db.pool,
        "TRANSFORM order_summaries FROM order_totals SELECT total + total AS grand_total",
        &order_totals_columns,
        "public",
    )
    .await
    .expect("install_definition records the definition and returns");
    assert_eq!(summary_def.status, TransformStatus::WaitingToBackfill);
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("the discharge dispatches the chunked build");
    assert_eq!(
        status_named(&client, summary_def.def.target.as_str()).await,
        "backfilling",
        "nothing drains the chunk queue in this test, so order_summaries must still be \
         backfilling"
    );

    // Trip `order_totals.total`'s own fuse (distinct ids from the 3 seeded
    // above, so as not to disturb them) — the real cascade path,
    // `defs::catalog::column_dependents`, must reach `order_summaries` here
    // exactly as it does in `a_dependent_transforms_column_cascades_to_paused_when_its_upstream_column_pauses`,
    // regardless of `order_summaries` still being `Backfilling`.
    let bad_ids: Vec<i64> = (101..=100 + DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;

    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the upstream column must have tripped its own fuse"
    );
    let downstream_status = column_status_row(&client, "order_summaries", "grand_total")
        .await
        .expect(
            "the cascade must reach order_summaries even though its definition is still \
             backfilling",
        );
    assert!(
        !downstream_status.0,
        "a purely cascaded pause is not order_summaries's own local fuse"
    );

    let result = quarantine::resume_column(&db.pool, "order_totals", "total").await;
    assert_eq!(
        result.expect("resuming the live upstream column must succeed"),
        vec![("order_totals".to_string(), "total".to_string())],
        "the cascaded dependent must NOT be resumed alongside it: its definition isn't live yet"
    );

    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "the resumed upstream column itself must no longer be paused"
    );
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some(),
        "the cascaded-onto column must remain paused: its definition still isn't live, so \
         resume_column must have left it exactly as it was rather than stranding or dropping it"
    );
}

// ---------------------------------------------------------------------
// (l) Issue #130, epic #127: `recompute_column`'s to-one relationship read
//     moved onto the settled parent projection (#129), same as the ordinary
//     forward-apply path (`staging::apply::compute`) — quarantine replay
//     (this call site) must see the exact same semantics as normal drain,
//     not a live re-read that happens to disagree with it.
// ---------------------------------------------------------------------

/// `resume_column`'s recompute (`recompute_column`, the second of
/// `build_relationship_context`'s two call sites) must resolve a to-one
/// relationship path the same way the live CDC-apply path now does (#130):
/// from the settled parent projection, not a live read of the parent. Proved
/// by renaming the live `categories` row *after* the projection has already
/// settled on its original name, with no reverse recompute or projection
/// advance in between (#131 doesn't exist yet) — a live read would pick up
/// the rename; the projection cannot, because #130 alone never advances a
/// projection row's data columns on a parent-side mutation.
#[tokio::test]
async fn resume_column_resolves_a_to_one_relationship_from_the_projection_not_live_state() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             insert into categories (id, name) values (10, 'Tech'); \
             create table articles (id integer primary key, category_id integer); \
             alter table articles replica identity full; \
             insert into articles (id, category_id) values (1, 10)",
        )
        .await
        .expect("create + seed tables");

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    // Front door (issue #40): `create_definition`'s own widen call (#129)
    // catches category 10's `name` = 'Tech' into the projection right here,
    // while it's still the row's only-ever value.
    let source_columns = numeric_columns(&["id", "category_id"]);
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &source_columns,
    )
    .await
    .expect("create to-one enrichment definition");

    let article_cat_def = TransformDef {
        target: "article_cat".to_string(),
        source: "articles".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "category_name".to_string(),
            expr: Expr::RelationshipPath {
                rel: "category".to_string(),
                column: "name".to_string(),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let pk = source_primary_key(&db.pool, "articles")
        .await
        .expect("introspect articles pk");
    create_target_table(
        &db.pool,
        &article_cat_def,
        "public",
        &pk,
        &source_columns,
        &article_cat_def.source,
    )
    .await
    .expect("create target table");

    // Sentinel, distinct from either the live or the projected name, so a
    // successful recompute is unambiguous.
    client
        .execute(
            "insert into article_cat (id, category_name) values (1, 'SENTINEL')",
            &[],
        )
        .await
        .expect("seed sentinel target row");

    // Reached past the mechanism, same convention as this file's other
    // tests: seed `column_status` directly rather than driving a real
    // failure to trip the fuse.
    client
        .execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('article_cat', 'category_name', 'synthetic pause', true)",
            &[],
        )
        .await
        .expect("seed column_status");

    // Rename the live category — no CDC staged, no reverse recompute, no
    // projection advance. Only #131 (not built) would keep the projection in
    // step with this.
    client
        .execute("update categories set name = 'Renamed' where id = 10", &[])
        .await
        .expect("mutate the live category without touching the projection");

    // Confirm the projection genuinely still disagrees with live truth
    // before asserting anything about the recompute's output — otherwise a
    // passing assertion below wouldn't distinguish "read the projection"
    // from "coincidentally read the right value some other way".
    let projection = relationship_projection(&db.pool, relationship.id)
        .await
        .expect("read projection catalog row")
        .expect("to-one relationship has a projection");
    let projected_name: Option<String> = client
        .query_one(
            &format!(
                "select name from {} where id = 10",
                projection.projection_table
            ),
            &[],
        )
        .await
        .expect("read projection row")
        .get(0);
    assert_eq!(
        projected_name.as_deref(),
        Some("Tech"),
        "sanity check: the projection must still hold the pre-rename name"
    );

    let resumed = quarantine::resume_column(&db.pool, "article_cat", "category_name")
        .await
        .expect("resume_column");
    assert_eq!(
        resumed,
        vec![("article_cat".to_string(), "category_name".to_string())]
    );

    let recomputed: Option<String> = client
        .query_one("select category_name from article_cat where id = 1", &[])
        .await
        .expect("read article_cat")
        .get(0);
    assert_eq!(
        recomputed.as_deref(),
        Some("Tech"),
        "recompute_column (the quarantine-replay call site) must resolve the to-one \
         relationship from the settled projection, exactly like normal drain's forward \
         path (#130) — not the live category row, which was renamed to 'Renamed' after \
         the projection had already settled"
    );
}

// ---------------------------------------------------------------------
// Issue #281: column attribution must resolve a *bare* `src_table` before
// asking the catalog which transforms read that source.
// ---------------------------------------------------------------------

/// Stages one malformed CDC insert per id under the **bare** `orders`
/// spelling — `insert_cdc_row`'s deliberate opposite (it qualifies every
/// `src_table` through `qualify_fixture_table`, precisely so this whole
/// file's machinery is exercised at all). This is what a durable pre-#267
/// ring row looks like, and what several of this crate's other integration
/// fixtures still stage by hand.
async fn stage_bad_orders_bare(client: &mut Client, pool: &trellis::Pool, ids: &[i64]) {
    let table = active_segment_table(client).await;
    for id in ids {
        client
            .execute(
                &format!(
                    "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                     values ('orders', $1, 'insert', $2, null, $3::text::jsonb, 0)"
                ),
                &[
                    &id.to_string(),
                    &PgLsn::from(1u64),
                    &Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
                ],
            )
            .await
            .expect("stage bare-src_table cdc row");
    }
    let seg_seq = seal_active_segment(client).await;
    let result = apply::drain_once(
        pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
        &StagedWatermark::saturated(),
    )
    .await;
    assert!(
        matches!(result, Err(ApplyError::Eval(_))),
        "a malformed numeric field must still surface as an evaluator failure for a bare \
         src_table (`apply::compute` resolves it via `qualified_schema_node_key`), got \
         {result:?}"
    );
}

/// A calculated-field failure on a row staged with a **bare** `src_table`
/// must still be attributed to its `(transform, column)` pair.
///
/// `attribute_column_failure` used to pass its raw `src_table` straight to
/// `catalog::transforms_for_source`, which requires an already-qualified name
/// (issue #74, ADR-0007) and answers a bare one with an *empty* candidate set
/// rather than an error. The lookup therefore matched no definition, the
/// function fell out through its "no candidate" arm, and the failure went
/// completely unattributed — indistinguishable, from the outside, from the
/// deliberate ambiguous-match fallback, and with no log line either.
///
/// Note `apply::compute` itself handles the same bare row fine (hence the
/// `ApplyError::Eval` this fixture asserts on): it routes every
/// `transforms_for_source` call through its own `qualified_schema_node_key`.
/// The gap was quarantine's two call sites alone. Pre-fix both assertions
/// below see `None`; post-fix the column is charged and, at the threshold,
/// paused.
#[tokio::test]
async fn a_bare_src_table_failure_is_still_attributed_to_its_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    assert_eq!(
        DEFAULT_COLUMN_DEATH_THRESHOLD, 5,
        "test assumes the default"
    );

    let ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders_bare(&mut client, &db.pool, &ids).await;

    let status = column_status_row(&client, "order_totals", "total")
        .await
        .expect(
            "a threshold's worth of evaluator failures on a bare `src_table` must pause the \
             column, not go unattributed (issue #281)",
        );
    assert!(status.0, "a threshold trip is a local fuse, not a cascade");
    assert!(status.1.is_some(), "the tripping error must be recorded");
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        None,
        "the counter must reset once the fuse trips"
    );
}

// ---------------------------------------------------------------------
// (g) The operator-driven half of a column pause — issue #227, ADR-0014's
//     "one state, two triggers" at column granularity, reached through the
//     unified `PAUSE TRANSFORM <target>.<column>` grammar rather than by a
//     fuse tripping.
// ---------------------------------------------------------------------

/// `PAUSE TRANSFORM <target>.<column>` must reach exactly the state the fuse
/// reaches — a `column_status` row plus a cascade onto every dependent reader
/// — so that a deliberately paused column freezes its value and is recovered
/// by the very same `RESUME`. Anything else would be the second freezing
/// mechanism ADR-0014 rules out.
#[tokio::test]
async fn pausing_a_column_by_statement_reaches_the_fuse_s_own_state_and_cascades() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    let applied = trellis
        .apply("PAUSE TRANSFORM order_totals.total")
        .await
        .expect("pause a healthy column deliberately");
    assert!(
        matches!(applied, trellis::Applied::Paused),
        "got {applied:?}"
    );

    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        Some((true, None)),
        "an operator pause records its own reason to stay paused (local_fuse), with no \
         error of its own — nothing failed"
    );
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        None,
        "no fuse tripped, so nothing may have been charged against one"
    );
    assert!(
        cascade_edge_exists(
            &client,
            "order_summaries",
            "grand_total",
            "order_totals",
            "total"
        )
        .await,
        "a dependent reader must pause too rather than silently consume a frozen value"
    );
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some(),
        "the cascaded dependent must actually be paused, not only edge-recorded"
    );

    // ADR-0014's idempotency clause: pause runs outside any host migration
    // transaction, so a replayed migration has to be safe to re-run.
    trellis
        .apply("PAUSE TRANSFORM order_totals.total")
        .await
        .expect("pausing an already-paused column is a success");
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        Some((true, None)),
    );

    // The same grammar's resume un-cascades the dependent along with it: the
    // dependent's only reason to be paused was this cascade.
    let resumed = trellis
        .apply("RESUME TRANSFORM order_totals.total")
        .await
        .expect("resume the deliberately-paused column");
    let trellis::Applied::Resumed { columns } = resumed else {
        panic!("a RESUME statement must report a resume, got {resumed:?}");
    };
    assert_eq!(
        columns,
        vec![
            ("order_totals".to_string(), "total".to_string()),
            ("order_summaries".to_string(), "grand_total".to_string()),
        ],
        "the addressed column first, then the dependent un-cascaded with it"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None
    );
    assert_eq!(
        column_status_row(&client, "order_summaries", "grand_total").await,
        None
    );
}

/// Issue #228's decision-5 clause about "addressing something that doesn't
/// exist": a column address is validated against the definition's own declared
/// fields before anything is written, because `column_status` has no foreign
/// key that would catch it — an unchecked pause would park a row naming
/// nothing and only surface on a later resume.
#[tokio::test]
async fn pausing_a_column_that_does_not_exist_is_refused_before_anything_is_written() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    let err = trellis
        .apply("PAUSE TRANSFORM order_totals.nonesuch")
        .await
        .expect_err("a column the definition never declared must be refused");
    assert_eq!(err.code(), trellis::ErrorCode::NotFound);
    let message = err.to_string();
    assert!(
        message.contains("nonesuch") && message.contains("total"),
        "the refusal must name both the bad column and the ones that exist: {message}"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "nonesuch").await,
        None,
        "a refused pause must leave no column_status row behind"
    );

    // And the whole-transform half of the same check: a dotted address whose
    // *transform* half is unknown. (This is also the shape a caller gets if
    // they wrote `<schema>.<transform>`, which this grammar reads as
    // `<transform>.<column>` — see `Trellis::apply`'s "Addressing".)
    let err = trellis
        .apply("PAUSE TRANSFORM no_such_transform.total")
        .await
        .expect_err("an unknown transform must be refused");
    assert_eq!(err.code(), trellis::ErrorCode::NotFound);
    assert!(err.to_string().contains("no_such_transform"), "got {err}");
}

/// Issue #305: `resume_column` must close its single-snapshot recompute with
/// the same parked catch-up a first `define`'s chunked build closes with
/// (`defs::catalog::complete_direct_backfill`), not just clear
/// `column_status` and hope nothing slipped past.
///
/// The race: [`quarantine::resume_column`]'s recompute reads the source once;
/// a row changed after that read, whose delta is applied before the column
/// unpauses, gets every *other* column updated by live CDC apply but leaves
/// the resumed column stale forever. The interleaving can't be forced from
/// outside `resume_column`, so this reproduces the state it leaves behind
/// (source row changed, resumed column still at its pre-change value) and
/// asserts the catch-up the resume parked repairs it.
#[tokio::test]
async fn resume_column_parks_a_catch_up_that_repairs_a_row_changed_mid_recompute() {
    use trellis::intake::publication;
    use trellis::staging::{has_pending, retire_drained_segments};

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_order_totals(&db, &client).await;

    client
        .execute("insert into orders (id, price, tax) values (1, 10, 5)", &[])
        .await
        .expect("seed a source row");
    // Stand-in for the target row live CDC apply already built for it —
    // `resume_column`'s recompute only fills in rows that exist.
    client
        .execute("insert into public.order_totals (id) values (1)", &[])
        .await
        .expect("seed the target row");
    quarantine::pause_column(&db.pool, "order_totals", "total")
        .await
        .expect("pause the column");
    let parked_before: i64 = client
        .query_one("select count(*) from pending_backfill", &[])
        .await
        .expect("count pending_backfill")
        .get(0);
    assert_eq!(parked_before, 0, "no marker is pending before the resume");

    quarantine::resume_column(&db.pool, "order_totals", "total")
        .await
        .expect("resume the column");
    let total: String = client
        .query_one(
            "select total::text from public.order_totals where id = 1",
            &[],
        )
        .await
        .expect("read the recomputed column")
        .get(0);
    assert_eq!(total, "15", "the recompute populated the row");

    // The state the race leaves behind: `orders.price` moved 10 -> 20 after
    // the recompute read row 1, and its delta was applied while `total` was
    // still paused, so `total` never followed it.
    client
        .execute("update orders set price = 20 where id = 1", &[])
        .await
        .expect("update the source row");

    let parked: i64 = client
        .query_one(
            "select count(*) from pending_backfill where table_name = $1",
            &[&format!("{DEFAULT_SCHEMA}.orders")],
        )
        .await
        .expect("read pending_backfill")
        .get(0);
    assert_eq!(
        parked, 1,
        "resume_column must park a catch-up marker for the definition's source table when it \
         unpauses the column, exactly as complete_direct_backfill does for a first define"
    );

    // Discharge it (retrying while the cluster-wide `xmin` fence settles) and
    // drain the enumeration it stages.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        publication::run_pending_backfills(
            &mut client,
            "trellis_column_quarantine_test",
            &StagedWatermark::saturated(),
            Duration::ZERO,
        )
        .await
        .expect("run_pending_backfills");
        let remaining: i64 = client
            .query_one("select count(*) from pending_backfill", &[])
            .await
            .expect("count pending_backfill")
            .get(0);
        if remaining == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "pending_backfill marker never settled"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for _ in 0..16 {
        let seg_seq = seal_active_segment(&mut client).await;
        while apply::drain_once(
            &db.pool,
            seg_seq,
            "worker",
            1,
            "trellis_column_quarantine_test",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(&mut client)
            .await
            .expect("retire drained segments");
        if !has_pending(&client).await.expect("has_pending") {
            break;
        }
    }

    let total: String = client
        .query_one(
            "select total::text from public.order_totals where id = 1",
            &[],
        )
        .await
        .expect("read the column after the catch-up")
        .get(0);
    assert_eq!(
        total, "25",
        "the catch-up must re-derive the resumed column for the row that changed mid-recompute"
    );
}
