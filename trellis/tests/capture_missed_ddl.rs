//! DDL that changes what a read column holds without firing a capture
//! trigger, and the routine operations that must not look
//! like it.
//!
//! Each case runs the step-1 study's operation (`local_docs/diag/703` in
//! the study's worktree; the ids below are its rows) on a table of its own,
//! calls the capture pass directly at each of the study's `PASS` markers,
//! and then checks what the pass did to the table's two readers: nothing,
//! a rebuild (`backfilling`, or `catching_up` reported downstream of one, counted in
//! `capture_auto_rebuilds_total`), or a pause. Nothing polls for
//! convergence.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

const SCHEMA: &str = "trellis";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

async fn definer(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer")
}

/// One whole staging-worker pass: capture, then the start of ready builds.
async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(raw, pool, SCHEMA)
        .await
        .expect("reconcile pass");
}

/// The capture half of one pass only.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = reconcile::reconcile(
        raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(2),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "capture_missed_ddl_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_missed_ddl_test",
            1,
            "trellis_capture_missed_ddl_test",
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
    panic!("the ring did not reach quiescence within 16 seal/drain rounds");
}

async fn run_backfill_chunks(pool: &trellis::Pool) {
    trellis::staging::build::settle_builds(pool).await;
}

/// Every definition's status, by bare target name.
async fn statuses(raw: &Client) -> BTreeMap<String, TransformStatus> {
    raw.query(
        "select split_part(target_table, '.', 2), status from transform_definitions",
        &[],
    )
    .await
    .expect("read statuses")
    .into_iter()
    .map(|row| {
        (
            row.get(0),
            TransformStatus::from_persisted(row.get(1)).expect("known status"),
        )
    })
    .collect()
}

/// Alternates staging-worker passes, builds and drains until every
/// definition is live: a bounded number of explicit steps.
async fn bring_live(raw: &mut Client, pool: &trellis::Pool) {
    for _ in 0..10 {
        full_pass(raw, pool).await;
        run_backfill_chunks(pool).await;
        drain_to_quiescence(pool, raw).await;
        let statuses = statuses(raw).await;
        if statuses.values().all(|s| *s == TransformStatus::Live) {
            return;
        }
    }
    panic!("not every definition went live: {:?}", statuses(raw).await);
}

async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

/// `capture_auto_rebuilds_total` for `table` and `cause`, from this
/// process's registry (tables are unique per test, so parallel tests don't
/// share a series).
fn auto_rebuilds(table: &str, cause: &str) -> u64 {
    let rendered = trellis::Metrics::new().render_prometheus();
    rendered
        .lines()
        .filter(|line| {
            line.starts_with("trellis_capture_auto_rebuilds_total{")
                && line.contains(&format!("source_table=\"{table}\""))
                && line.contains(&format!("cause=\"{cause}\""))
        })
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|n| n.parse::<f64>().ok())
                .expect("a counter value") as u64
        })
        .sum()
}

/// What the pass should have done to a case's two readers: `r_<case>`, a 1-1
/// passthrough of `a`, `b`, `v`, `n` and `x`, and `s_<case>`, `SUM(b)` by
/// `g`. `x` was added with a constant default, which its `pg_attribute` row
/// keeps until the table is rewritten.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// Neither paused nor rebuilt.
    Nothing,
    /// Both rebuilt, for `cause`.
    Rebuilt(&'static str),
    /// `r` paused, `s` untouched.
    PausedR,
    /// `s` paused, `r` untouched.
    PausedS,
    /// Both paused.
    PausedBoth,
}

/// One study row: its id, its statements (`{t}` the case's table) with
/// `-- PASS` between reconcile windows, and what the final rules do.
type Case = (&'static str, &'static str, Expect);

/// Creates each case's table and readers, brings them live, then runs each
/// case in turn and checks its outcome.
async fn run_cases(cases: &[Case]) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    for (id, _, _) in cases {
        let id = id.to_lowercase();
        let t = format!("t_{id}");
        raw.batch_execute(&format!(
            "create table public.{t} (id int primary key, g int not null, a int, b int, \
                 v varchar(50), n numeric(10,2), u int); \
             insert into public.{t} select i, i % 3, i, i, 'v' || i, i + 0.5, i \
               from generate_series(1, 30) i; \
             alter table public.{t} add column x text default 'x';"
        ))
        .await
        .expect("create the case's table");
        for text in [
            format!(
                "TRANSFORM r_{id} FROM public.{t} \
                 SELECT a AS a, b AS b, v AS v, n AS n, x AS x"
            ),
            format!(
                "TRANSFORM s_{id} FROM public.{t} GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(b) AS total"
            ),
        ] {
            trellis.apply(&text).await.expect(&text);
        }
    }
    bring_live(&mut raw, &db.pool).await;

    let app = connect(db.dsn()).await;
    let mut failures = Vec::new();
    for (id, script, expect) in cases {
        let t = format!("t_{}", id.to_lowercase());
        let script = script.replace("{t}", &t);
        for window in script.split("-- PASS") {
            for statement in window.split(';').map(str::trim).filter(|s| !s.is_empty()) {
                app.batch_execute(statement)
                    .await
                    .unwrap_or_else(|e| panic!("{id}: {statement}: {e}"));
            }
            capture_pass(&mut raw, &db.pool).await;
        }
        // A second pass after the last window: a check that pauses leaves
        // the table to the next pass, which runs the checks after it.
        capture_pass(&mut raw, &db.pool).await;

        let statuses = statuses(&raw).await;
        let r = statuses[&format!("r_{}", id.to_lowercase())];
        let s = statuses[&format!("s_{}", id.to_lowercase())];
        let rebuilding = |status: TransformStatus| {
            matches!(
                status,
                TransformStatus::CatchingUp | TransformStatus::Backfilling
            )
        };
        let table = format!("public.{t}");
        let rebuilds =
            auto_rebuilds(&table, "value_rewrite") + auto_rebuilds(&table, "column_changed");
        let got = match (r, s) {
            (TransformStatus::Live, TransformStatus::Live) if rebuilds == 0 => {
                Some(Expect::Nothing)
            }
            (r, s) if rebuilding(r) && rebuilding(s) => {
                if auto_rebuilds(&table, "value_rewrite") > 0 {
                    Some(Expect::Rebuilt("value_rewrite"))
                } else if auto_rebuilds(&table, "column_changed") > 0 {
                    Some(Expect::Rebuilt("column_changed"))
                } else {
                    None
                }
            }
            (TransformStatus::Paused, TransformStatus::Live) if rebuilds == 0 => {
                Some(Expect::PausedR)
            }
            (TransformStatus::Live, TransformStatus::Paused) if rebuilds == 0 => {
                Some(Expect::PausedS)
            }
            (TransformStatus::Paused, TransformStatus::Paused) if rebuilds == 0 => {
                Some(Expect::PausedBoth)
            }
            _ => None,
        };
        if got != Some(*expect) {
            failures.push(format!(
                "{id}: expected {expect:?}, got r {r:?}, s {s:?}, {rebuilds} rebuilds"
            ));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Part `i` of `n` of `cases` (every case lands in exactly one part), so a
/// long list runs as several tests, each on its own cluster, in parallel.
fn part(cases: &[Case], i: usize, n: usize) -> &[Case] {
    let size = cases.len().div_ceil(n);
    &cases[(i * size).min(cases.len())..((i + 1) * size).min(cases.len())]
}

/// Maintenance alone, and maintenance in one window with a metadata
/// `ALTER`: none is a change to what a column holds.
fn maintenance_cases() -> Vec<Case> {
    vec![
        ("M1", "vacuum full public.{t}", Expect::Nothing),
        ("M2", "cluster public.{t} using {t}_pkey", Expect::Nothing),
        ("M3", "reindex table public.{t}", Expect::Nothing),
        (
            "M4",
            "analyze public.{t}; vacuum public.{t}; vacuum (analyze) public.{t}",
            Expect::Nothing,
        ),
        (
            "M5",
            "delete from public.{t} where id % 2 = 0; vacuum public.{t}; \
             insert into public.{t} (id, g, a, b, v, n, x, u) select i, 1, i, i, 'v', 1, 'x', i \
               from generate_series(2, 30, 2) i",
            Expect::Nothing,
        ),
        (
            "M6",
            "vacuum freeze pg_catalog.pg_attribute",
            Expect::Nothing,
        ),
        ("M7", "vacuum full pg_catalog.pg_attribute", Expect::Nothing),
        (
            "M8",
            "vacuum freeze pg_catalog.pg_attribute; vacuum full public.{t}",
            Expect::Nothing,
        ),
        (
            "M9",
            "vacuum full public.{t}; vacuum freeze pg_catalog.pg_attribute",
            Expect::Nothing,
        ),
        (
            "M10",
            "vacuum freeze pg_catalog.pg_attribute -- PASS vacuum full public.{t}",
            Expect::Nothing,
        ),
        (
            "M11",
            "alter table public.{t} alter column a set default 5 -- PASS vacuum full public.{t}",
            Expect::Nothing,
        ),
        (
            "M12",
            "alter table public.{t} alter column a set default 5; vacuum full public.{t}",
            Expect::Nothing,
        ),
        (
            "M13",
            "alter table public.{t} alter column a set statistics 500; \
             cluster public.{t} using {t}_pkey",
            Expect::Nothing,
        ),
    ]
}

#[tokio::test]
async fn maintenance_rebuilds_and_pauses_nothing_part_1() {
    run_cases(part(&maintenance_cases(), 0, 2)).await;
}

#[tokio::test]
async fn maintenance_rebuilds_and_pauses_nothing_part_2() {
    run_cases(part(&maintenance_cases(), 1, 2)).await;
}

/// Single migrations. A pure widening never rebuilds or pauses here, except
/// that an integer width a copy is outgrown by pauses `r` by the copy-type check
/// (a copy has no modifier, ADR-0020, so a modifier never outgrows one). A
/// narrowed `varchar` and a `numeric` scale change rebuild.
fn single_migration_cases() -> Vec<Case> {
    vec![
        (
            "D1",
            "alter table public.{t} alter column a set default 7",
            Expect::Nothing,
        ),
        (
            "D2",
            "alter table public.{t} alter column x drop default",
            Expect::Nothing,
        ),
        (
            "D3",
            "alter table public.{t} alter column a set not null",
            Expect::Nothing,
        ),
        (
            "D4",
            "alter table public.{t} alter column a drop not null",
            Expect::Nothing,
        ),
        (
            "D5",
            "alter table public.{t} alter column a set statistics 300",
            Expect::Nothing,
        ),
        (
            "D6",
            "alter table public.{t} alter column x set storage external",
            Expect::Nothing,
        ),
        (
            "D7",
            "alter table public.{t} alter column v type varchar(255)",
            Expect::Nothing,
        ),
        (
            "D8",
            "alter table public.{t} alter column v type text",
            Expect::Nothing,
        ),
        // The passthrough's `integer` copy is outgrown.
        (
            "D9",
            "alter table public.{t} alter column a type bigint",
            Expect::PausedR,
        ),
        (
            "D10",
            "alter table public.{t} alter column n type numeric(12,2)",
            Expect::Nothing,
        ),
        // ADR-0020: the passthrough's copy is an unconstrained `numeric`,
        // which holds every scale. The rewrite re-renders the stored values
        // (`1.50` as `1.5000`, or rounded), so the column's readers rebuild.
        (
            "D11",
            "alter table public.{t} alter column n type numeric(10,4)",
            Expect::Rebuilt("column_changed"),
        ),
        (
            "D12",
            "alter table public.{t} alter column n type numeric(10,1)",
            Expect::Rebuilt("column_changed"),
        ),
        (
            "D13",
            "alter table public.{t} add column z double precision default random()",
            Expect::Nothing,
        ),
        (
            "D14",
            "alter table public.{t} add column z int default 3",
            Expect::Nothing,
        ),
        (
            "D15",
            "alter table public.{t} alter column u type bigint",
            Expect::Nothing,
        ),
        (
            "D16",
            "create index {t}_a_idx on public.{t} (a)",
            Expect::Nothing,
        ),
        (
            "D17",
            "create index {t}_a_idx on public.{t} (a) -- PASS drop index public.{t}_a_idx",
            Expect::Nothing,
        ),
        (
            "D18",
            "alter table public.{t} add constraint {t}_ck check (a > 0)",
            Expect::Nothing,
        ),
        (
            "D19",
            "alter table public.{t} add constraint {t}_uq unique (b)",
            Expect::Nothing,
        ),
        (
            "D20",
            "create unique index {t}_pk2 on public.{t} (id); \
             begin; alter table public.{t} drop constraint {t}_pkey, \
                 add constraint {t}_pkey primary key using index {t}_pk2; commit",
            Expect::Nothing,
        ),
        (
            "D21",
            "grant select (a) on public.{t} to public",
            Expect::Nothing,
        ),
        // R1: a narrowed modifier. Every definition is still valid, and the
        // passthrough's copy holds every value, so the readers
        // rebuild.
        (
            "D22",
            "alter table public.{t} alter column v type varchar(10)",
            Expect::Rebuilt("column_changed"),
        ),
        // `s`'s `GROUP BY` copy is outgrown.
        (
            "D23",
            "alter table public.{t} alter column g type bigint",
            Expect::PausedS,
        ),
        // R1: a narrower scale renders the stored values with another number
        // of fractional digits (`1.50` as `1.5`): the readers of the column
        // rebuild; `s` doesn't read `n`.
        (
            "D24",
            "alter table public.{t} alter column n type numeric(9,1)",
            Expect::Rebuilt("column_changed"),
        ),
    ]
}

#[tokio::test]
async fn single_migrations_act_only_on_a_changed_column_part_1() {
    run_cases(part(&single_migration_cases(), 0, 3)).await;
}

#[tokio::test]
async fn single_migrations_act_only_on_a_changed_column_part_2() {
    run_cases(part(&single_migration_cases(), 1, 3)).await;
}

#[tokio::test]
async fn single_migrations_act_only_on_a_changed_column_part_3() {
    run_cases(part(&single_migration_cases(), 2, 3)).await;
}

/// Migrations paired in one transaction, or one window, the way framework
/// migrations run them: R3b ignores a rewrite whose column row changed
/// visibly (P1, P2, P4, P7, X3, X4), which plain R3 paused for.
fn paired_migration_cases() -> Vec<Case> {
    vec![
        (
            "P1",
            "begin; alter table public.{t} alter column a set default 9; \
             alter table public.{t} add column z double precision default random(); commit",
            Expect::Nothing,
        ),
        (
            "P2",
            "begin; alter table public.{t} alter column a set not null; \
             alter table public.{t} add column z double precision default random(); commit",
            Expect::Nothing,
        ),
        (
            "P3",
            "begin; alter table public.{t} alter column v type varchar(255); \
             alter table public.{t} add column z double precision default random(); commit",
            Expect::Nothing,
        ),
        (
            "P4",
            "begin; alter table public.{t} alter column a set default 9; \
             alter table public.{t} alter column u type bigint; commit",
            Expect::Nothing,
        ),
        (
            "P5",
            "begin; alter table public.{t} add column z int default 3; \
             alter table public.{t} alter column a set statistics 200; commit",
            Expect::Nothing,
        ),
        (
            "P6",
            "begin; alter table public.{t} add column z double precision default random(); commit",
            Expect::Nothing,
        ),
        (
            "P7",
            "begin; alter table public.{t} add column z uuid default gen_random_uuid(); \
             alter table public.{t} alter column x set default 'y'; commit",
            Expect::Nothing,
        ),
        (
            "P8",
            "alter table public.{t} alter column a set default 9 -- PASS \
             alter table public.{t} add column z double precision default random()",
            Expect::Nothing,
        ),
        (
            "X2",
            "alter table public.{t} set unlogged; alter table public.{t} set logged",
            Expect::Nothing,
        ),
        (
            "X3",
            "begin; alter table public.{t} alter column a set default 1; \
             alter table public.{t} set unlogged; commit; alter table public.{t} set logged",
            Expect::Nothing,
        ),
        (
            "X4",
            "begin; alter table public.{t} add column gen int generated always as (a * 2) stored; \
             alter table public.{t} alter column a set not null; commit",
            Expect::Nothing,
        ),
        (
            "X6",
            "alter table public.{t} drop constraint {t}_pkey, \
                 add constraint {t}_pkey primary key (id)",
            Expect::Nothing,
        ),
        (
            "X7",
            "alter table public.{t} rename constraint {t}_pkey to {t}_pk_renamed",
            Expect::Nothing,
        ),
        (
            "X8",
            "alter table public.{t} alter column v type varchar(255); vacuum full public.{t}",
            Expect::Nothing,
        ),
        (
            "X9",
            "alter table public.{t} alter column a set not null -- PASS vacuum full public.{t}",
            Expect::Nothing,
        ),
        (
            "X10",
            "begin; alter table public.{t} alter column a set default 2; \
             alter table public.{t} set tablespace pg_default; commit",
            Expect::Nothing,
        ),
    ]
}

#[tokio::test]
async fn paired_migrations_rebuild_and_pause_nothing_part_1() {
    run_cases(part(&paired_migration_cases(), 0, 2)).await;
}

#[tokio::test]
async fn paired_migrations_rebuild_and_pause_nothing_part_2() {
    run_cases(part(&paired_migration_cases(), 1, 2)).await;
}

/// The study's controls, and R3b's documented residue.
fn real_change_cases() -> Vec<Case> {
    vec![
        // R1: dropped and re-added with the same type; every definition is
        // still valid, so the readers rebuild.
        (
            "C1",
            "begin; alter table public.{t} drop column a; \
             alter table public.{t} add column a int; commit",
            Expect::Rebuilt("column_changed"),
        ),
        // A pure widening R1 leaves alone; the copy-type check pauses both readers, whose
        // copies of `b` it outgrew. A reader with no such copy misses the
        // values (docs/known-correctness-gaps.md).
        (
            "C2",
            "alter table public.{t} alter column b type bigint using b * 10",
            Expect::PausedBoth,
        ),
        // R3b.
        (
            "C3",
            "alter table public.{t} alter column b type int using b * 10",
            Expect::Rebuilt("value_rewrite"),
        ),
        // No rewrite at all.
        (
            "C4",
            "alter table public.{t} alter column b type int using b",
            Expect::Nothing,
        ),
        // R2, in the pass.
        (
            "C5",
            "alter table public.{t} drop constraint {t}_pkey, add primary key (id, g)",
            Expect::PausedBoth,
        ),
        (
            "C6",
            "update public.{t} set g = id; \
             alter table public.{t} drop constraint {t}_pkey, add primary key (g)",
            Expect::PausedBoth,
        ),
        // DML a trigger captures.
        (
            "C7",
            "truncate public.{t}; insert into public.{t} (id, g, a, b, v, n, x, u) \
               select i, 1, i + 1, i, 'v', 1, 'x', i from generate_series(1, 30) i",
            Expect::Nothing,
        ),
        (
            "C8",
            "alter table public.{t} alter column b set default 3 -- PASS \
             alter table public.{t} alter column b type int using b * 10",
            Expect::Rebuilt("value_rewrite"),
        ),
        (
            "C9",
            "alter table public.{t} alter column b type int using b * 10; \
             vacuum freeze pg_catalog.pg_attribute",
            Expect::Rebuilt("value_rewrite"),
        ),
        // Residual false positive: an identity rewrite.
        (
            "X1",
            "alter table public.{t} alter column b type int using b + 0",
            Expect::Rebuilt("value_rewrite"),
        ),
        (
            "X5",
            "alter table public.{t} drop constraint {t}_pkey, add primary key (id, g) -- PASS \
             alter table public.{t} drop constraint {t}_pkey, add primary key (g, id)",
            Expect::PausedBoth,
        ),
        // Residual false positive: an identical default re-applied in the
        // window of a rewrite.
        (
            "Y1",
            "alter table public.{t} alter column a set default 5 -- PASS \
             begin; alter table public.{t} alter column a set default 5; \
             alter table public.{t} add column z double precision default random(); commit",
            Expect::Rebuilt("value_rewrite"),
        ),
        (
            "Y2",
            "alter table public.{t} alter column b set default 3; \
             alter table public.{t} alter column b set not null -- PASS \
             alter table public.{t} alter column b type int using b * 10",
            Expect::Rebuilt("value_rewrite"),
        ),
        (
            "Y3",
            "alter table public.{t} alter column b set default 3 -- PASS \
             alter table public.{t} alter column b type int using b + 0",
            Expect::Rebuilt("value_rewrite"),
        ),
        (
            "Y4",
            "begin; alter table public.{t} alter column a set default 1; \
             alter table public.{t} alter column b type int using b * 10; commit",
            Expect::Rebuilt("value_rewrite"),
        ),
        // The documented misses. A column added with a constant default
        // keeps it in its `pg_attribute` row until the table is rewritten,
        // and the rewrite clears it: a rewrite of the column then changed
        // its row. (Any rewrite clears it, `VACUUM FULL` included, which is
        // why the row can't leave it out: M1.)
        (
            "F1",
            "alter table public.{t} alter column x type text using x || '!'",
            Expect::Nothing,
        ),
        // A column with a compression method set: `ALTER COLUMN ... TYPE`
        // resets `attcompression` to the default, so the rewrite changed
        // its row.
        (
            "F2",
            "alter table public.{t} alter column v set compression pglz -- PASS \
             alter table public.{t} alter column v type varchar(50) using v || '!'",
            Expect::Nothing,
        ),
        // A metadata change to the same column in the rewrite's window.
        (
            "Z1",
            "begin; alter table public.{t} alter column b set default 3; \
             alter table public.{t} alter column b type int using b * 10; commit",
            Expect::Nothing,
        ),
        (
            "Z2",
            "alter table public.{t} alter column b set statistics 500; \
             alter table public.{t} alter column b type int using b * 10",
            Expect::Nothing,
        ),
        (
            "Z3",
            "alter table public.{t} alter column b type int using b * 10; \
             alter table public.{t} alter column b set statistics 500",
            Expect::Nothing,
        ),
    ]
}

#[tokio::test]
async fn real_changes_rebuild_or_pause_part_1() {
    run_cases(part(&real_change_cases(), 0, 3)).await;
}

#[tokio::test]
async fn real_changes_rebuild_or_pause_part_2() {
    run_cases(part(&real_change_cases(), 1, 3)).await;
}

#[tokio::test]
async fn real_changes_rebuild_or_pause_part_3() {
    run_cases(part(&real_change_cases(), 2, 3)).await;
}

/// R3b end to end: the same-type rewrite rebuilds both readers, which
/// converge on the rewritten values with no write to the table.
#[tokio::test]
async fn a_same_type_rewrite_rebuilds_its_readers_to_the_new_values() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.w (id int primary key, g int not null, b int); \
         insert into public.w select i, i % 3, i from generate_series(1, 30) i;",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in [
        "TRANSFORM wb FROM public.w SELECT b AS b",
        "TRANSFORM wsum FROM public.w GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(b) AS total",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool).await;

    raw.batch_execute("alter table public.w alter column b type int using b * 10")
        .await
        .expect("rewrite b");
    capture_pass(&mut raw, &db.pool).await;
    let statuses = statuses(&raw).await;
    for target in ["wb", "wsum"] {
        assert!(
            matches!(
                statuses[target],
                TransformStatus::CatchingUp | TransformStatus::Backfilling
            ),
            "{target}: {:?}",
            statuses[target]
        );
    }
    assert_eq!(auto_rebuilds("public.w", "value_rewrite"), 1);

    bring_live(&mut raw, &db.pool).await;
    assert_eq!(
        rows(&raw, "select id::text, b::text from public.wb order by id").await,
        rows(&raw, "select id::text, b::text from public.w order by id").await,
    );
    assert_eq!(
        rows(
            &raw,
            "select g::text, total::text from public.wsum order by g"
        )
        .await,
        rows(
            &raw,
            "select g::text, sum(b)::text from public.w group by g order by g"
        )
        .await,
    );
    // The baseline moved with it: the next pass rebuilds nothing more.
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(auto_rebuilds("public.w", "value_rewrite"), 1);
}

/// R1 by the validity rule, on readers with no typed copy of the column: a
/// change define still accepts rebuilds (a `SUM` argument's scale, a column
/// dropped and re-added as the same type); one it refuses, or one that
/// changes the type a definition evaluates the column as, pauses with a
/// `capture_failure` naming the column and the change.
#[tokio::test]
async fn a_retyped_field_rebuilds_while_valid_and_pauses_otherwise() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.f (id int primary key, g int not null, n numeric(10,2), \
             k text, q int); \
         insert into public.f select i, i % 3, i + 0.25, 'k' || i, i \
           from generate_series(1, 30) i;",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in [
        "TRANSFORM fsum FROM public.f GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(n) AS total",
        "TRANSFORM fk FROM public.f GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, COUNT(k) AS ks",
        "TRANSFORM fq FROM public.f GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, COUNT(q) AS qs",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    // A `SUM` argument's scale changed between two `numeric(p,s)` types.
    app.batch_execute("alter table public.f alter column n type numeric(12,4)")
        .await
        .expect("rescale n");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let statuses_now = statuses(&raw).await;
    assert!(
        matches!(
            statuses_now["fsum"],
            TransformStatus::CatchingUp | TransformStatus::Backfilling
        ),
        "{statuses_now:?}"
    );
    assert_eq!(auto_rebuilds("public.f", "column_changed"), 1);
    bring_live(&mut raw, &db.pool).await;
    // Equal as numbers: the rebuild keeps a group whose sum didn't change
    // as it was, rendered at the old scale.
    assert_eq!(
        rows(
            &raw,
            "select g::text, total::numeric(20,4)::text from public.fsum order by g"
        )
        .await,
        rows(
            &raw,
            "select g::text, sum(n)::numeric(20,4)::text from public.f group by g order by g"
        )
        .await,
    );

    // `q` moved to another type family: `COUNT(q)` is still valid, but the
    // definition evaluates `q` as an integer, so it pauses.
    app.batch_execute("alter table public.f alter column q type double precision")
        .await
        .expect("retype q");
    capture_pass(&mut raw, &db.pool).await;
    let reported = trellis.status("fq").await.expect("status").expect("fq");
    assert_eq!(reported.status, TransformStatus::Paused);
    let failure = reported.capture_failure.expect("the reason is reported");
    assert_eq!(failure.source_table, "public.f");
    assert_eq!(failure.columns, vec!["q".to_string()]);
    assert!(
        failure.error.contains("\"q\"")
            && failure.error.contains("integer")
            && failure.error.contains("double precision"),
        "{failure:?}"
    );
    let statuses_now = statuses(&raw).await;
    assert_eq!(statuses_now["fsum"], TransformStatus::Live);
    assert_eq!(statuses_now["fk"], TransformStatus::Live);

    // `k` dropped and re-added as the same type: every row's `k` is now
    // NULL, and the readers rebuild to it.
    app.batch_execute(
        "begin; alter table public.f drop column k; alter table public.f add column k text; commit",
    )
    .await
    .expect("drop and re-add k");
    capture_pass(&mut raw, &db.pool).await;
    assert!(matches!(
        statuses(&raw).await["fk"],
        TransformStatus::CatchingUp | TransformStatus::Backfilling
    ));
    assert_eq!(auto_rebuilds("public.f", "column_changed"), 2);
    bring_live_except(&mut raw, &db.pool, &["fq"]).await;
    assert_eq!(
        rows(&raw, "select g::text, ks::text from public.fk order by g").await,
        rows(
            &raw,
            "select g::text, count(k)::text from public.f group by g order by g"
        )
        .await,
    );
}

/// [`bring_live`], leaving `paused` paused.
async fn bring_live_except(raw: &mut Client, pool: &trellis::Pool, paused: &[&str]) {
    for _ in 0..10 {
        full_pass(raw, pool).await;
        run_backfill_chunks(pool).await;
        drain_to_quiescence(pool, raw).await;
        let statuses = statuses(raw).await;
        if statuses
            .iter()
            .all(|(t, s)| paused.contains(&t.as_str()) || *s == TransformStatus::Live)
        {
            return;
        }
    }
    panic!("not every definition went live: {:?}", statuses(raw).await);
}

/// R3b on a partitioned source: the root has no storage, so the baseline is
/// kept per leaf, and a `USING` on the root, which rewrites every leaf,
/// rebuilds the readers. A leaf attached later is recorded by the install
/// that covers it, and rebuilds nothing.
#[tokio::test]
async fn a_same_type_rewrite_of_a_partitioned_source_rebuilds_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.p (id int not null, g int not null, b int, primary key (g, id)) \
             partition by list (g); \
         create table public.p_0 partition of public.p for values in (0); \
         create table public.p_1 partition of public.p for values in (1); \
         insert into public.p select i, i % 2, i from generate_series(1, 30) i;",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    let text = "TRANSFORM psum FROM public.p GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(b) AS total";
    trellis.apply(text).await.expect(text);
    bring_live(&mut raw, &db.pool).await;
    let leaves = "select string_agg(distinct node::regclass::text, ',' order by node::regclass::text) \
                  from capture_baselines where table_name = 'public.p'";
    assert_eq!(
        rows(&raw, leaves).await,
        vec![vec![Some("p_0,p_1".to_string())]]
    );

    raw.batch_execute(
        "create table public.p_2 partition of public.p for values in (2); \
         insert into public.p values (100, 2, 7);",
    )
    .await
    .expect("attach a leaf");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        rows(&raw, leaves).await,
        vec![vec![Some("p_0,p_1,p_2".to_string())]]
    );
    assert_eq!(auto_rebuilds("public.p", "value_rewrite"), 0);
    bring_live(&mut raw, &db.pool).await;

    raw.batch_execute("alter table public.p alter column b type int using b * 10")
        .await
        .expect("rewrite b");
    capture_pass(&mut raw, &db.pool).await;
    assert!(matches!(
        statuses(&raw).await["psum"],
        TransformStatus::CatchingUp | TransformStatus::Backfilling
    ));
    assert_eq!(auto_rebuilds("public.p", "value_rewrite"), 1);
    bring_live(&mut raw, &db.pool).await;
    assert_eq!(
        rows(
            &raw,
            "select g::text, total::text from public.psum order by g"
        )
        .await,
        rows(
            &raw,
            "select g::text, sum(b)::text from public.p group by g order by g"
        )
        .await,
    );
}
