//! `MIN`/`MAX` on a group-basis aggregate without the index its probe
//! reads (#1117 rule 4, #1679): a retraction that reaches a group's extreme
//! re-reads it from the source, as one endpoint of an index on the
//! `GROUP BY` columns then the argument. When that index is dropped after
//! define, or doesn't serve the probe (partial, invalid, other leading
//! columns, another direction or collation), or the definition opted out
//! with `WITHOUT SOURCE INDEX`, the same probe scans the group instead: the
//! target stays exact, `status` warns (`extreme_index_gaps`) and nothing
//! pauses. The warning clears, and the probe reads the endpoint again, once
//! the index exists.
//!
//! Every test steps its pages by hand through `support/drain_driver.rs` and
//! reads `status` and the probe's plan directly: nothing sleeps or polls.
//! Define refuses `MIN`/`MAX` until its gate lands, so each test admits them
//! first (`trellis::staging::admit_min_max_for_tests`).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::{Config, DefinitionStatus, TransformStatus, Trellis, TrellisOptions};

const SRC: &str = "public.src";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g \
                   SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n";
const AGG_OPT_OUT: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
                           SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n";
/// The `GROUP BY` index `self_check` pages by, and the probe's index.
const INDEXED: &str = "create table public.src (id integer primary key, g integer, v integer); \
                       create index src_g_idx on public.src (g); \
                       create index src_g_v_idx on public.src (g, v)";
const ACTUAL: &str = "select g, lo, hi, n from public.agg order by g";
const EXPECTED: &str = "select g, min(v), max(v), count(*) from public.src group by g order by g";
const SEED: &str = "insert into public.src values \
                    (1, 1, 1), (2, 1, 5), (3, 1, 9), (4, null, 2), (5, null, 6), (6, null, 8), \
                    (7, 2, 3), (8, 2, 4)";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn start(definition: &str, source_ddl: &str, g: ValueType) -> Driver {
    trellis::staging::admit_min_max_for_tests();
    Driver::start(
        source_ddl,
        &[("id", int4()), ("g", g), ("v", int4())],
        &[definition],
        &[SRC],
    )
    .await
}

/// A `Trellis` on the driver's database that runs no staging worker: it
/// only reads `status`.
async fn handle(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect")
}

async fn status(t: &Trellis) -> DefinitionStatus {
    t.status("agg").await.expect("status").expect("defined")
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql`, drains it as one sealed page and asserts the target equals
/// `GROUP BY` over the source.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        "{sql}: the target (left) differs from the oracle (right)"
    );
}

/// The probe's plan.
async fn probe_plan(d: &Driver) -> String {
    trellis::dev::staging::explain_group_probe(d.pool(), "agg")
        .await
        .expect("explain the probe")
}

/// The probe's reads of the source: its plan's scans of `src`.
fn source_reads(plan: &str) -> Vec<&str> {
    let reads: Vec<&str> = plan.lines().filter(|l| l.contains(" on src s")).collect();
    assert!(!reads.is_empty(), "the probe reads the source:\n{plan}");
    reads
}

/// Whether a node of the probe's plan sorts source rows: a read that
/// scans a group and sorts it for its extreme.
fn sorts_source_rows(plan: &str) -> bool {
    let sort = regex::Regex::new(r"Sort Key: s(_\d+)?\.").expect("regex");
    plan.lines().any(|l| sort.is_match(l))
}

/// The probe reads one endpoint of `index` per extreme: the index serves
/// every read of the source and none sorts source rows.
fn assert_endpoint(plan: &str, index: &str) {
    for read in source_reads(plan) {
        assert!(read.contains(index), "{read} reads {index}:\n{plan}");
    }
    assert!(!sorts_source_rows(plan), "no read sorts its group:\n{plan}");
}

/// The probe scans each group and sorts it for the extreme, through no
/// read of `missing`.
fn assert_group_scan(plan: &str, missing: &str) {
    for read in source_reads(plan) {
        assert!(!read.contains(missing), "{missing} is gone: {read}\n{plan}");
    }
    assert!(
        sorts_source_rows(plan),
        "each read sorts its group:\n{plan}"
    );
}

/// The one `MIN`/`MAX` index gap, on `(g, v)` for `v`.
fn assert_one_gap(status: &DefinitionStatus, opted_out: bool) {
    assert_eq!(status.extreme_index_gaps.len(), 1, "{status:?}");
    let gap = &status.extreme_index_gaps[0];
    assert_eq!(gap.table, "public.src");
    assert_eq!(gap.columns, ["g", "v"]);
    assert_eq!(gap.argument, "v");
    assert_eq!(gap.opted_out, opted_out);
    assert_eq!(gap.fix(), r#"create index on "public"."src" ("g", "v")"#);
}

/// The issue's test: the probe's index dropped under a live definition.
/// Retractions of a minimum and a maximum, in a group and in the `NULL`
/// group, stay exact by a scan of the group through the `GROUP BY` index;
/// `status` names the index and nothing pauses. Re-created, the warning
/// clears and the probe reads the index's endpoint again.
#[tokio::test]
async fn a_dropped_probe_index_scans_the_group_warns_and_pauses_nothing() {
    let mut d = start(AGG, INDEXED, int4()).await;
    let t = handle(&d).await;
    step(&mut d, SEED).await;
    assert!(status(&t).await.extreme_index_gaps.is_empty());
    assert_endpoint(&probe_plan(&d).await, "src_g_v_idx");

    write(&d, "drop index public.src_g_v_idx").await;
    let dropped = status(&t).await;
    assert_one_gap(&dropped, false);
    assert!(
        dropped.source_index_gaps.is_empty(),
        "self_check's index is still there: {dropped:?}"
    );
    let plan = probe_plan(&d).await;
    assert_group_scan(&plan, "src_g_v_idx");
    assert!(plan.contains("src_g_idx"), "by the GROUP BY index:\n{plan}");
    for sql in [
        "delete from public.src where id = 1",
        "delete from public.src where id = 3",
        "delete from public.src where id = 4",
        "update public.src set v = null where id = 6",
        "delete from public.src where id = 7",
    ] {
        step(&mut d, sql).await;
    }
    let after = status(&t).await;
    assert_eq!(after.status, TransformStatus::Live, "nothing pauses");
    assert!(after.capture_failure.is_none(), "{after:?}");
    assert!(after.drain_failure.is_none(), "{after:?}");
    assert_one_gap(&after, false);

    write(&d, "create index src_g_v_idx on public.src (g, v)").await;
    assert!(status(&t).await.extreme_index_gaps.is_empty());
    assert_endpoint(&probe_plan(&d).await, "src_g_v_idx");
    step(&mut d, "delete from public.src where id in (2, 5, 8)").await;
    assert_eq!(status(&t).await.status, TransformStatus::Live);
}

/// An index that doesn't serve the probe counts as missing: the `GROUP BY`
/// columns alone, a partial one, one leading with the argument, one whose
/// columns run in different directions, and an invalid one. Through each
/// the target stays exact.
#[tokio::test]
async fn an_index_that_does_not_serve_the_probe_counts_as_missing() {
    let mut d = start(AGG, INDEXED, int4()).await;
    let t = handle(&d).await;
    step(&mut d, SEED).await;
    write(&d, "drop index public.src_g_v_idx").await;
    // Only the `GROUP BY` index is left.
    assert_one_gap(&status(&t).await, false);
    let mut next = 100;
    for ddl in [
        // Partial.
        "create index x on public.src (g, v) where v is not null",
        // The argument first.
        "create index x on public.src (v, g)",
        // The columns in different directions.
        "create index x on public.src (g, v desc)",
    ] {
        write(&d, ddl).await;
        assert_one_gap(&status(&t).await, false);
        // A new minimum in group 1 and in the `NULL` group, then its
        // retraction, which probes both groups.
        step(
            &mut d,
            &format!(
                "insert into public.src values ({next}, 1, 0), ({}, null, 0)",
                next + 1
            ),
        )
        .await;
        step(
            &mut d,
            &format!("delete from public.src where id in ({next}, {})", next + 1),
        )
        .await;
        write(&d, "drop index public.x").await;
        next += 2;
    }

    // An index a `create index concurrently` left invalid: built and ready,
    // never validated. A failed validation phase leaves exactly this; the
    // catalog flag stands in for that race.
    write(&d, "create index x on public.src (g, v)").await;
    assert!(status(&t).await.extreme_index_gaps.is_empty());
    d.ctl
        .batch_execute(
            "update pg_catalog.pg_index set indisvalid = false \
             where indexrelid = 'public.x'::regclass",
        )
        .await
        .expect("mark the index invalid");
    assert_one_gap(&status(&t).await, false);
    assert_group_scan(&probe_plan(&d).await, "using x on");
    // Group 1's minimum.
    step(&mut d, "delete from public.src where id = 1").await;
    write(&d, "drop index public.x").await;

    // Descending throughout serves the probe (read backward), and a
    // trailing column doesn't stop it counting.
    write(&d, "create index x on public.src (g desc, v desc, id)").await;
    assert!(status(&t).await.extreme_index_gaps.is_empty());
    assert_endpoint(&probe_plan(&d).await, "using x on");
}

/// A text `GROUP BY` column: an index on it under another collation than
/// the column's doesn't serve the probe, which reads the group under the
/// column's own collation, so it counts as missing. One under the column's
/// collation does.
#[tokio::test]
async fn an_index_under_another_collation_counts_as_missing() {
    let mut d = start(
        AGG,
        "create table public.src (id integer primary key, g text, v integer); \
         create index src_g_idx on public.src (g)",
        ValueType::Text,
    )
    .await;
    let t = handle(&d).await;
    step(
        &mut d,
        "insert into public.src values (1, 'a', 1), (2, 'a', 5), (3, null, 2), (4, null, 7)",
    )
    .await;
    assert_one_gap(&status(&t).await, false);
    write(&d, r#"create index x on public.src (g collate "C", v)"#).await;
    assert_one_gap(&status(&t).await, false);
    step(&mut d, "delete from public.src where id in (1, 4)").await;
    write(
        &d,
        "drop index public.x; create index x on public.src (g, v)",
    )
    .await;
    assert!(status(&t).await.extreme_index_gaps.is_empty());
    assert_endpoint(&probe_plan(&d).await, "using x on");
    step(&mut d, "delete from public.src where id in (2, 3)").await;
}

/// A definition defined `WITHOUT SOURCE INDEX` over a source with no index
/// but its key probes by the same statement, which then scans for the
/// group: exact, `NULL` group included. `status` reports both indexes as
/// opted out. Once the probe's index exists, its gap clears and the probe
/// reads the endpoint.
#[tokio::test]
async fn an_opted_out_definition_probes_by_scanning_for_the_group() {
    let mut d = start(
        AGG_OPT_OUT,
        "create table public.src (id integer primary key, g integer, v integer)",
        int4(),
    )
    .await;
    let t = handle(&d).await;
    step(&mut d, SEED).await;
    let opted = status(&t).await;
    assert_one_gap(&opted, true);
    assert_eq!(opted.source_index_gaps.len(), 1, "{opted:?}");
    assert!(opted.source_index_gaps[0].opted_out);
    assert_group_scan(&probe_plan(&d).await, "src_g_v_idx");
    for sql in [
        "delete from public.src where id = 3",
        "delete from public.src where id = 4",
        "delete from public.src where id = 7",
    ] {
        step(&mut d, sql).await;
    }
    assert_eq!(status(&t).await.status, TransformStatus::Live);

    write(&d, "create index src_g_v_idx on public.src (g, v)").await;
    let indexed = status(&t).await;
    assert!(indexed.extreme_index_gaps.is_empty(), "{indexed:?}");
    assert!(indexed.source_index_gaps.is_empty(), "{indexed:?}");
    assert_endpoint(&probe_plan(&d).await, "src_g_v_idx");
    step(&mut d, "delete from public.src where id in (1, 6)").await;
}

/// A definition with no `MIN` or `MAX` reports no such gap and reads no
/// index for it, whatever indexes its source has.
#[tokio::test]
async fn a_definition_without_min_or_max_reports_no_gap() {
    let mut d = start(
        "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n",
        "create table public.src (id integer primary key, g integer, v integer); \
         create index src_g_idx on public.src (g)",
        int4(),
    )
    .await;
    let t = handle(&d).await;
    write(&d, SEED).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
    let s = status(&t).await;
    assert!(s.extreme_index_gaps.is_empty(), "{s:?}");
    assert!(s.source_index_gaps.is_empty(), "{s:?}");
}
