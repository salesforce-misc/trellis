//! Issue #104 pin: the aggregate-write path's staged `Recompute` rows must
//! carry a real `src_changed` origin, the same way issues #51/#52 already
//! fixed for the 1-1 propagation path (`end_to_end_latency.rs`, this file's
//! sibling, pins that one). Before this fix, `apply_aggregate`'s
//! `AggregateTargetPlan`/written-group shape carried no origin at all, so
//! `apply.rs`'s "3b" step always staged an aggregate write's downstream
//! `Recompute` with `src_changed: None` — silently blanking both the
//! per-transform and end-to-end latency histograms for any transform
//! chained off an aggregate target. This was believed moot (no definition
//! could survive its first drain attempt reading from an aggregate target's
//! composite key) until issue #103 disproved that for single-group-by-column
//! aggregates and was fixed, making the gap live rather than moot.
//!
//! Structurally this mirrors `end_to_end_latency.rs`'s
//! `end_to_end_latency_fires_only_at_the_terminal_transform_in_a_linear_chain`
//! almost exactly — same two-hop-chain shape, same metric assertions —
//! except hop 0 is an aggregate write (`sales -> sku_totals`, `GROUP BY sku`)
//! rather than a 1-1 transform, and hop 1 (`sku_totals -> sku_totals_v2`,
//! itself also an aggregate) is reached purely through the aggregate-write
//! path's own automatic downstream propagation (`apply.rs`'s "3b" step),
//! not the 1-1 propagation path #51/#52 already covered. Schema/helper
//! conventions (`qualify_fixture_table`, `replica identity full`,
//! `active_seg_table`) are cribbed from
//! `defs_aggregate_chained_single_column_group_key.rs`, the front-door
//! aggregate-chaining fixture issue #103's fix already exercises.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
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
/// `defs_aggregate_chained_single_column_group_key.rs`'s helper of the same
/// name.
fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
}

/// Stages one image-bearing (CDC-shaped) change into the active ring
/// segment, with `src_changed` set to `now()` — mirroring
/// `end_to_end_latency.rs`'s `insert_cdc_row` (these tests are specifically
/// about the latency histograms that only fire off a non-`NULL`
/// `src_changed`), but resolving the active segment dynamically like
/// `defs_aggregate_chained_single_column_group_key.rs`'s `stage_cdc` does,
/// since by the time this runs, both aggregate definitions' own installs
/// have already sealed/drained past `seg_0`.
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
        .unwrap_or_else(|e| panic!("stage cdc {key:?} into {table} failed: {e}"));
}

/// Seals and drains repeatedly until nothing is pending anywhere in the ring
/// — used only for the two definitions' own (empty, no source rows yet)
/// from-scratch backfills, not for the hops this test actually inspects
/// metrics around (those drain one segment at a time, below).
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "e2e_latency_aggregate_test",
            1,
            "trellis_e2e_latency_aggregate_test",
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

/// Drains exactly one sealed segment, once, for the two hops this test
/// inspects metrics between — mirrors `end_to_end_latency.rs`'s own `drain`
/// helper.
async fn drain(pool: &trellis::Pool, seg_seq: i64) -> apply::ApplyOutcome {
    let watermark = trellis::staging::StagedWatermark::saturated();
    apply::drain_once(
        pool,
        seg_seq,
        "e2e_latency_aggregate_test",
        1,
        "trellis_e2e_latency_aggregate_test",
        &watermark,
    )
    .await
    .expect("drain_once")
    .expect("drain_once must claim and drain something")
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
    "TRANSFORM e2e_latency_agg_sku_totals FROM sales GROUP BY sku SELECT sum(amount) AS total";
const SKU_TOTALS_V2: &str = "TRANSFORM e2e_latency_agg_sku_totals_v2 FROM e2e_latency_agg_sku_totals \
     GROUP BY sku SELECT sum(total) AS total2";

/// A one-histogram-family text scrape of `rendered` for `metric{transform="target"}`
/// — see `end_to_end_latency.rs`'s identical helper.
fn metric_mentions_transform(rendered: &str, metric: &str, target: &str) -> bool {
    rendered
        .lines()
        .any(|line| line.starts_with(metric) && line.contains(&format!("transform=\"{target}\"")))
}

/// The cumulative count in one histogram bucket (`le="<bound>"`) for
/// `metric{transform="target"}` — see `end_to_end_latency.rs`'s identical
/// helper.
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

/// Linear chain of two **aggregates**:
/// `sales -> e2e_latency_agg_sku_totals -> e2e_latency_agg_sku_totals_v2`,
/// the second (chained onto the first aggregate's own composite/PK identity)
/// being the only terminal transform. Hop 1 is driven purely by the
/// aggregate-write path's own automatic downstream propagation — no second,
/// independently-staged change — exactly the shape issue #104 is about.
#[tokio::test]
async fn end_to_end_latency_fires_at_the_terminal_transform_chained_off_an_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table sales (id integer primary key, sku text, amount numeric); \
             alter table sales replica identity full",
        )
        .await
        .expect("create the aggregate source table");

    install_definition(&db.pool, SKU_TOTALS, &sales_columns(), "public")
        .await
        .expect("install the upstream aggregate");
    drain_to_quiescence(&db.pool, &mut client).await;

    client
        .batch_execute("alter table e2e_latency_agg_sku_totals replica identity full")
        .await
        .expect("widen the upstream aggregate target's replica identity");

    install_definition(&db.pool, SKU_TOTALS_V2, &sku_totals_columns(), "public")
        .await
        .expect("install the aggregate chained onto the upstream aggregate");
    drain_to_quiescence(&db.pool, &mut client).await;

    // The one source-committed change this whole test traces: a brand-new
    // group's only row.
    client
        .execute(
            "insert into sales (id, sku, amount) values (1, 'a', 10)",
            &[],
        )
        .await
        .expect("seed the source row after both definitions exist");
    stage_cdc(
        &client,
        "sales",
        "1",
        "insert",
        None,
        Some(r#"{"id":"1","sku":"a","amount":"10"}"#),
    )
    .await;

    // Hop 0: sales -> e2e_latency_agg_sku_totals (intermediate aggregate;
    // has a downstream reader).
    let seg1 = seal_active_segment(&mut client).await;
    let outcome1 = drain(&db.pool, seg1).await;
    assert_eq!(outcome1.keys_written, 1, "one new group written");

    let after_hop0 = trellis::metrics::Metrics::new().render_prometheus();
    assert!(
        metric_mentions_transform(
            &after_hop0,
            "trellis_transform_latency_seconds",
            "e2e_latency_agg_sku_totals",
        ),
        "the intermediate aggregate hop must still get its own per-transform latency \
         (issue #51): {after_hop0}"
    );
    assert!(
        !metric_mentions_transform(
            &after_hop0,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_agg_sku_totals",
        ),
        "an intermediate hop (has a downstream reader) must never get an end-to-end \
         observation: {after_hop0}"
    );

    // A real (short, but non-trivial) delay before draining hop 1, so the
    // assertions after it can tell a *traced-through* latency (at least this
    // old) apart from one a bug might freshly stamp at hop 1's own apply
    // time (which would show up near-zero instead).
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Hop 1: e2e_latency_agg_sku_totals -> e2e_latency_agg_sku_totals_v2
    // (terminal), driven *purely* by the `Recompute` row the upstream
    // aggregate's own write staged automatically (`apply.rs`'s "3b" step +
    // downstream-propagation step 4) — no second, independently-staged
    // change anywhere. Before issue #104's fix, `AggregateTargetPlan`'s
    // written-group shape carried no `src_changed`, so this `Recompute` row
    // always staged with `src_changed: None` and neither histogram could
    // observe anything here.
    let seg2 = seal_active_segment(&mut client).await;
    let outcome2 = drain(&db.pool, seg2).await;
    assert_eq!(
        outcome2.keys_written, 1,
        "the chained group written downstream"
    );

    let after_hop1 = trellis::metrics::Metrics::new().render_prometheus();
    assert!(
        metric_mentions_transform(
            &after_hop1,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_agg_sku_totals_v2",
        ),
        "issue #104: the terminal transform chained off an aggregate target must get an \
         end-to-end observation once it's reached purely through the aggregate-write path's \
         own automatic downstream propagation, with no manufactured second write: {after_hop1}"
    );
    assert_eq!(
        bucket_count(
            &after_hop1,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_agg_sku_totals_v2",
            "0.1",
        ),
        0,
        "the observation must be traced back to the original sales commit (at least the \
         150ms this test slept before draining hop 1), not freshly stamped ~0 at hop 1's own \
         apply time — proving the Recompute carried a real src_changed, not None: {after_hop1}"
    );
    assert!(
        !metric_mentions_transform(
            &after_hop1,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_agg_sku_totals",
        ),
        "the intermediate aggregate hop must still never get an end-to-end observation: \
         {after_hop1}"
    );
    assert!(
        metric_mentions_transform(
            &after_hop1,
            "trellis_transform_latency_seconds",
            "e2e_latency_agg_sku_totals_v2",
        ),
        "the terminal transform's own per-transform latency (issue #51) must be recorded too, \
         now that this hop — reached only via a Recompute staged from an aggregate write — \
         carries a real origin: {after_hop1}"
    );
}
