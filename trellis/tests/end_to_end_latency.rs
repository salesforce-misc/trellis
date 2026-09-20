//! Integration tests for issue #52's end-to-end latency histogram (ADR-0009
//! decision 2): time from a source commit to a *terminal* transform's apply,
//! keyed by terminal transform only.
//!
//! Mirrors `apply.rs`'s conventions: source/target tables and definitions
//! built by hand, changes staged directly into the ring, drains run via
//! `apply::drain_once` (intake is out of scope here, same as `apply.rs`).
//! `apply.rs`'s `a_change_propagates_two_hops_downstream_then_stops` is the
//! DAG-shape fixture these tests crib from — that test already proves
//! propagation stops at a transform with no downstream reader; these tests
//! layer `trellis::metrics::Metrics::new().render_prometheus()` assertions on top of the same
//! shape to prove the *metric* also only fires there.
//!
//! Transform/target names below are deliberately distinctive
//! (`e2e_latency_*`) rather than reusing `apply.rs`'s `order_totals`/
//! `order_summary` names: `trellis::metrics`'s registry is a single
//! process-wide global (see `metrics.rs`'s module doc comment), shared by
//! every test in this binary, so a name any other test also uses as a
//! *terminal* target could taint an assertion here that a given label never
//! received an end-to-end observation.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{
    create_definition, create_target_table, require_single_column_pk, source_primary_key,
};
use trellis::staging::apply;

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
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

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// Stages one image-bearing (CDC-shaped) change directly into `table`,
/// mirroring `apply.rs`'s helper of the same name — except this one also
/// sets `src_changed` to `now()` (`apply.rs`'s own helper leaves it `NULL`,
/// which is fine for its purely functional assertions, but these tests are
/// specifically about the latency histograms that only fire off a non-NULL
/// `src_changed`, per [`FoldedChange::src_changed`]'s doc comment).
async fn insert_cdc_row(
    client: &Client,
    table: &str,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen, \
                 src_changed) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0, now())"
            ),
            &[&src_table, &key, &op, &lsn, &old_image, &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("insert cdc row {key:?} into {table} failed: {e}"));
}

async fn drain(pool: &trellis::Pool, seg_seq: i64, claimed_by: &str) -> apply::ApplyOutcome {
    // Issue #132: a throwaway, always-caught-up watermark — no live
    // `Intake` runs in this test, and this file isn't exercising guard (a).
    let watermark = trellis::staging::StagedWatermark::saturated();
    apply::drain_once(
        pool,
        seg_seq,
        claimed_by,
        1,
        "trellis_e2e_latency_test",
        &watermark,
    )
    .await
    .expect("drain_once")
    .expect("drain_once must claim and drain something")
}

/// A one-histogram-family text scrape of `rendered` for `metric{transform="target"}`
/// — enough to tell whether `target` ever received an observation under
/// `metric` without parsing full Prometheus text exposition.
fn metric_mentions_transform(rendered: &str, metric: &str, target: &str) -> bool {
    rendered
        .lines()
        .any(|line| line.starts_with(metric) && line.contains(&format!("transform=\"{target}\"")))
}

/// The cumulative count in one histogram bucket (`le="<bound>"`) for
/// `metric{transform="target"}` — used where presence alone
/// ([`metric_mentions_transform`]) isn't enough and a test needs to pin down
/// *which* bucket an observation actually landed in (proving a latency was
/// traced through to a real, older origin rather than freshly stamped at
/// the hop that recorded it).
fn bucket_count(rendered: &str, metric: &str, target: &str, le: &str) -> u64 {
    let bucket_metric = format!("{metric}_bucket");
    rendered
        .lines()
        .find(|line| {
            line.starts_with(&bucket_metric)
                && line.contains(&format!("transform=\"{target}\""))
                && line.contains(&format!("le=\"{le}\""))
        })
        .and_then(|line| line.rsplit(' ').next())
        .unwrap_or_else(|| panic!("no {bucket_metric} bucket le={le} for {target} in: {rendered}"))
        .parse()
        .expect("bucket value parses as an integer")
}

/// Linear chain: `orders -> e2e_latency_chain_totals -> e2e_latency_chain_summary`,
/// the second definition being the only terminal transform.
///
/// This exercises the **real** automatic hop-to-hop propagation path — a
/// single source-committed CDC row against `orders`, no manufactured second
/// write anywhere. `apply_and_mark_drained`'s downstream propagation (step
/// 4, `trellis/src/staging/apply.rs`) stages the second hop's
/// `StagedChange::Recompute` row itself; prior to the multi-hop-gap fix
/// (issues #51/#52, discovered during #52's review), `Recompute` carried no
/// `src_changed` of its own (see `staging::append::StagedChange`'s doc
/// comment), so a transform reached only through one — the common case for
/// any hop beyond the first — folded to a `FoldedChange` with
/// `src_changed: None`, and neither histogram could observe anything for
/// it. `Recompute` now threads the triggering change's `src_changed`
/// forward (`apply.rs`'s `ChangedKey`/`TargetWrite::src_changed`/
/// `TargetDelete::src_changed`), so this test proves the terminal transform
/// gets a genuine end-to-end observation — traced all the way back to the
/// original `orders` commit — once it's reached purely through automatic
/// propagation, with no second, independently-staged change involved.
#[tokio::test]
async fn end_to_end_latency_fires_only_at_the_terminal_transform_in_a_linear_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let totals_def = TransformDef {
        target: "e2e_latency_chain_totals".to_string(),
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
    };
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM e2e_latency_chain_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create chain_totals definition");
    let pk = require_single_column_pk(
        source_primary_key(&db.pool, &totals_def.source)
            .await
            .expect("introspect source primary key"),
        &totals_def.source,
    )
    .expect("single-column pk");
    create_target_table(
        &db.pool,
        &totals_def,
        "public",
        &pk,
        &source_columns,
        &totals_def.source,
    )
    .await
    .expect("create chain_totals table");

    // The terminal hop: reads chain_totals, has no downstream reader itself.
    let totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = create_definition(
        &db.pool,
        "TRANSFORM e2e_latency_chain_summary FROM e2e_latency_chain_totals \
         SELECT total + total AS grand_total",
        &totals_columns,
    )
    .await
    .expect("create chain_summary definition");
    create_target_table(
        &db.pool,
        &summary_def.def,
        "public",
        &pk,
        &totals_columns,
        &summary_def.def.source,
    )
    .await
    .expect("create chain_summary table");

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed source rows after both definitions exist");

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

    // Hop 0: orders -> chain_totals (intermediate; has a downstream reader).
    let seg1 = seal_active_segment(&mut client).await;
    let outcome1 = drain(&db.pool, seg1, "worker").await;
    assert_eq!(outcome1.keys_written, 1);

    let after_hop0 = trellis::metrics::Metrics::new().render_prometheus();
    assert!(
        metric_mentions_transform(
            &after_hop0,
            "trellis_transform_latency_seconds",
            "e2e_latency_chain_totals",
        ),
        "the intermediate hop must still get its own per-transform latency (issue #51): {after_hop0}"
    );
    assert!(
        !metric_mentions_transform(
            &after_hop0,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_chain_totals",
        ),
        "an intermediate hop (has a downstream reader) must never get an end-to-end \
         observation: {after_hop0}"
    );

    // A real (short, but non-trivial) delay before draining hop 1 below, so
    // the assertions after it can tell a *traced-through* latency (which
    // must be at least this old) apart from one a bug might freshly stamp
    // at hop 1's own apply time (which would show up near-zero instead).
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Hop 1: chain_totals -> chain_summary (terminal), driven *purely* by
    // the `Recompute` row chain_totals' own apply staged automatically
    // (`apply_and_mark_drained`'s downstream propagation) — no second,
    // independently-staged change anywhere. This is the real automatic
    // hop-to-hop propagation path issues #51/#52's multi-hop gap was about:
    // before the fix, this `Recompute` row carried no `src_changed`, so
    // neither histogram could observe anything here; now it carries hop 0's
    // own origin forward.
    let seg2 = seal_active_segment(&mut client).await;
    let outcome2 = drain(&db.pool, seg2, "worker").await;
    assert_eq!(outcome2.keys_written, 1);

    let after_hop1 = trellis::metrics::Metrics::new().render_prometheus();
    assert!(
        metric_mentions_transform(
            &after_hop1,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_chain_summary",
        ),
        "the terminal transform must get an end-to-end observation once it's reached purely \
         through automatic hop-to-hop propagation, with no manufactured second write: {after_hop1}"
    );
    assert_eq!(
        bucket_count(
            &after_hop1,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_chain_summary",
            "0.1",
        ),
        0,
        "the observation must be traced back to the original orders commit (at least the \
         150ms this test slept before draining hop 1), not freshly stamped ~0 at hop 1's own \
         apply time: {after_hop1}"
    );
    assert!(
        !metric_mentions_transform(
            &after_hop1,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_chain_totals",
        ),
        "the intermediate hop must still never get an end-to-end observation, even though it \
         (like the terminal transform) now has a real `src_changed` to observe against: \
         {after_hop1}"
    );
    assert!(
        metric_mentions_transform(
            &after_hop1,
            "trellis_transform_latency_seconds",
            "e2e_latency_chain_summary",
        ),
        "the terminal transform's own per-transform latency (issue #51) must be recorded too, \
         now that hop 2 — reached only via Recompute — carries a real origin: {after_hop1}"
    );
}

/// DAG fan-out: one source (`orders`) feeding two terminal transforms
/// directly (`e2e_latency_fanout_a`, `e2e_latency_fanout_b`), neither read
/// by anything else. One drain of one source-committed change must record
/// an end-to-end observation for *both* terminal transforms.
#[tokio::test]
async fn end_to_end_latency_fires_for_every_terminal_transform_in_a_fan_out() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let source_columns = numeric_columns(&["id", "price", "tax"]);

    let def_a = TransformDef {
        target: "e2e_latency_fanout_a".to_string(),
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
    };
    create_definition(
        &db.pool,
        "TRANSFORM e2e_latency_fanout_a FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create fanout_a definition");
    let pk = require_single_column_pk(
        source_primary_key(&db.pool, &def_a.source)
            .await
            .expect("introspect source primary key"),
        &def_a.source,
    )
    .expect("single-column pk");
    create_target_table(
        &db.pool,
        &def_a,
        "public",
        &pk,
        &source_columns,
        &def_a.source,
    )
    .await
    .expect("create fanout_a table");

    let def_b = TransformDef {
        target: "e2e_latency_fanout_b".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "price_only".to_string(),
            expr: Expr::Column("price".to_string()),
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    create_definition(
        &db.pool,
        "TRANSFORM e2e_latency_fanout_b FROM orders SELECT price AS price_only",
        &source_columns,
    )
    .await
    .expect("create fanout_b definition");
    create_target_table(
        &db.pool,
        &def_b,
        "public",
        &pk,
        &source_columns,
        &def_b.source,
    )
    .await
    .expect("create fanout_b table");

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed source rows after both definitions exist");

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

    let seg1 = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg1, "worker").await;
    assert_eq!(outcome.keys_written, 2, "one write per fan-out branch");

    let rendered = trellis::metrics::Metrics::new().render_prometheus();
    assert!(
        metric_mentions_transform(
            &rendered,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_fanout_a",
        ),
        "terminal branch a must get an end-to-end observation: {rendered}"
    );
    assert!(
        metric_mentions_transform(
            &rendered,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_fanout_b",
        ),
        "terminal branch b must get an end-to-end observation: {rendered}"
    );
}
