//! Capture triggers or functions changed outside Trellis, on a partitioned
//! source (#1200, slice 2 of #707): a disruption of the root's or any node's
//! capture pauses both readers of the source, and partition maintenance of
//! every routine kind pauses nothing.
//!
//! Each case runs on a partitioned table of its own, `p_<case>`, a two-level
//! tree, with two readers: `r_<case>`, a `COUNT(*)` by `a` (not the
//! partition key), and `s_<case>`, `SUM(b)` by `g`. (A 1-1 transform over a
//! partitioned source is refused at define, so aggregates are the readers.)
//! The case's windows run between calls of the capture pass, which is called
//! directly. Nothing polls for convergence (#297).

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
        seal::seal_phase2(client, outcome.sealed_seg_seq, "capture_disruption_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_disruption_test",
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

/// What a case should leave its two readers as.
#[derive(Debug, Clone, Copy)]
enum Expect {
    /// Neither reader paused, no `capture_failure`.
    Nothing,
    /// Both paused, with a `capture_failure` containing this text.
    Paused(&'static str),
}

/// One case: its id, its windows (one batch of statements each, `{t}` the
/// case's root, a capture pass after each) and what the pass should do.
type Case = (String, Vec<String>, Expect);

/// The case's two-level tree: `p` partitioned by list on `g`; `l1` a leaf,
/// `l2` hash-partitioned on `g` into `l2h0` and `l2h1`, `l3` a leaf.
fn tree_ddl(t: &str) -> String {
    format!(
        "create table public.{t} (id int not null, g int not null, a int, b int, u int, \
             primary key (g, id)) partition by list (g); \
         create table public.{t}_l1 partition of public.{t} for values in (0); \
         create table public.{t}_l2 partition of public.{t} for values in (1) \
             partition by hash (g); \
         create table public.{t}_l2h0 partition of public.{t}_l2 \
             for values with (modulus 2, remainder 0); \
         create table public.{t}_l2h1 partition of public.{t}_l2 \
             for values with (modulus 2, remainder 1); \
         create table public.{t}_l3 partition of public.{t} for values in (2); \
         insert into public.{t} select i, i % 3, i, i, i from generate_series(1, 30) i;"
    )
}

/// A `DO` block running `action` (with `%I` for the trigger name) on each of
/// `rel`'s (`{t}`-relative) Trellis capture triggers.
fn each_trigger(rel: &str, action: &str) -> String {
    format!(
        "do $$ declare r record; begin \
           for r in select tgname from pg_trigger \
                    where tgrelid = 'public.{rel}'::regclass and tgname like 'trellis_capture_%' \
           loop execute format('{}', r.tgname); end loop; end $$",
        action.replace("{rel}", rel)
    )
}

/// A `DO` block that drops and creates each capture trigger of `public.<rel>`
/// again, `ENABLE ALWAYS`: new oids, the same definitions.
fn recreate_triggers(rel: &str) -> String {
    format!(
        "do $$ declare r record; begin \
           for r in select tgname, pg_get_triggerdef(oid) as def from pg_trigger \
                    where tgrelid = 'public.{rel}'::regclass and tgname like 'trellis_capture_%' \
           loop execute format('drop trigger %I on public.{rel}', r.tgname); \
                execute r.def; \
                execute format('alter table public.{rel} enable always trigger %I', r.tgname); \
           end loop; end $$"
    )
}

async fn create_case_tables(raw: &Client, trellis: &Trellis, ids: &[String]) {
    for id in ids {
        let t = format!("p_{id}");
        raw.batch_execute(&tree_ddl(&t))
            .await
            .expect("create the case's tree");
        for text in [
            format!(
                "TRANSFORM r_{id} FROM public.{t} GROUP BY g, a WITHOUT SOURCE INDEX SELECT g AS g, a AS a, COUNT(*) AS n"
            ),
            format!(
                "TRANSFORM s_{id} FROM public.{t} GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(b) AS total"
            ),
        ] {
            trellis.apply(&text).await.expect(&text);
        }
    }
}

/// Every reader's `(status, capture_failure)` after two passes; the second
/// must give the first's reason.
async fn settle(
    raw: &mut Client,
    pool: &trellis::Pool,
    trellis: &Trellis,
    id: &str,
    failures: &mut Vec<String>,
) -> Vec<(TransformStatus, Option<String>)> {
    let mut reasons = Vec::new();
    for _ in 0..2 {
        capture_pass(raw, pool).await;
        let mut now = Vec::new();
        for reader in ["r", "s"] {
            let status = trellis.status(&format!("{reader}_{id}")).await.unwrap();
            let status = status.expect("the reader exists");
            // A live reader's pause is a `disruption` (#1181), so the
            // client test helper's `resume_capture_disruptions` resumes it.
            if let Some(f) = &status.capture_failure
                && f.kind != trellis::CaptureFailureKind::Disruption
            {
                failures.push(format!("{id}: {reader} paused with kind {:?}", f.kind));
            }
            now.push((status.status, status.capture_failure.map(|f| f.error)));
        }
        reasons.push(now);
    }
    // A second pass never changes the reason.
    if reasons[0] != reasons[1] {
        failures.push(format!("{id}: the reason changed: {reasons:?}"));
    }
    reasons.pop().unwrap()
}

async fn run_cases(cases: &[Case]) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    let ids: Vec<String> = cases.iter().map(|(id, _, _)| id.to_lowercase()).collect();
    create_case_tables(&raw, &trellis, &ids).await;
    bring_live(&mut raw, &db.pool).await;

    let app = connect(db.dsn()).await;
    let mut failures = Vec::new();
    for (id, windows, expect) in cases {
        let id = id.to_lowercase();
        let t = format!("p_{id}");
        for window in windows {
            app.batch_execute(&window.replace("{t}", &t))
                .await
                .unwrap_or_else(|e| panic!("{id}: {window}: {e:?}"));
            capture_pass(&mut raw, &db.pool).await;
        }
        let now = settle(&mut raw, &db.pool, &trellis, &id, &mut failures).await;
        match expect {
            Expect::Nothing => {
                if now.iter().any(|(_, f)| f.is_some())
                    || now.iter().any(|(s, _)| *s == TransformStatus::Paused)
                {
                    failures.push(format!("{id}: expected nothing, got {now:?}"));
                }
            }
            Expect::Paused(text) => {
                let ok = now.iter().all(|(s, f)| {
                    *s == TransformStatus::Paused && f.as_deref().is_some_and(|f| f.contains(text))
                });
                if !ok {
                    failures.push(format!(
                        "{id}: expected a pause naming {text:?}, got {now:?}"
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

fn w(windows: &[&str]) -> Vec<String> {
    windows.iter().map(|w| w.to_string()).collect()
}

/// Every slice-1 disruption applied to `rel` (`label` names its cases) pauses
/// both readers on the first pass that sees it.
async fn every_disruption_pauses(label: &str, rel: &str) {
    let mut cases: Vec<Case> = Vec::new();
    let kinds: Vec<(&str, String, &'static str)> = vec![
        (
            "drop",
            each_trigger(rel, &format!("drop trigger %I on public.{rel}")),
            "is gone",
        ),
        (
            "recreate",
            recreate_triggers(rel),
            "dropped and created again",
        ),
        (
            "disable",
            each_trigger(rel, &format!("alter table public.{rel} disable trigger %I")),
            "is disabled",
        ),
        (
            "origin",
            format!(
                "alter table public.{rel} disable trigger user; \
                 alter table public.{rel} enable trigger user"
            ),
            "ORIGIN mode",
        ),
        (
            "replica",
            each_trigger(
                rel,
                &format!("alter table public.{rel} enable replica trigger %I"),
            ),
            "REPLICA mode",
        ),
        (
            "alter",
            each_trigger(
                rel,
                &format!(
                    "alter table public.{rel} disable trigger %1$I; \
                     alter table public.{rel} enable always trigger %1$I"
                ),
            ),
            "was altered",
        ),
    ];
    for (kind, window, text) in kinds {
        cases.push((
            format!("{label}_{kind}"),
            vec![window],
            Expect::Paused(text),
        ));
    }
    run_cases(&cases).await;
}

/// The root, a leaf, a middle node and a leaf under it each get every
/// disruption kind in their own test (own cluster), so the harness runs them
/// in parallel.
#[tokio::test]
async fn a_disruption_of_the_root_pauses_both_readers() {
    every_disruption_pauses("root", "{t}").await;
}

#[tokio::test]
async fn a_disruption_of_a_leaf_pauses_both_readers() {
    every_disruption_pauses("leaf", "{t}_l1").await;
}

#[tokio::test]
async fn a_disruption_of_a_middle_node_pauses_both_readers() {
    every_disruption_pauses("mid", "{t}_l2").await;
}

#[tokio::test]
async fn a_disruption_of_a_leaf_under_a_middle_node_pauses_both_readers() {
    every_disruption_pauses("deep", "{t}_l2h0").await;
}

/// A replaced capture function, and a node's triggers named in the finding.
#[tokio::test]
async fn a_replaced_function_and_a_named_node_pause_both_readers() {
    let cases: Vec<Case> = vec![
        (
            "fn_replaced".into(),
            vec![
                "do $$ declare r record; begin \
               for r in select distinct tgfoid::regproc::text as f from pg_trigger \
                        where tgrelid = 'public.{t}'::regclass and tgname like 'trellis_capture_%' \
               loop execute format('create or replace function %s() returns trigger \
                    language plpgsql security definer as $b$ begin return null; end $b$', r.f); \
               end loop; end $$"
                    .into(),
            ],
            Expect::Paused("was replaced"),
        ),
        // A node's triggers named in the finding.
        (
            "names_node".into(),
            vec![each_trigger("{t}_l3", "drop trigger %I on public.{t}_l3")],
            Expect::Paused("(on public.p_names_node_l3)"),
        ),
    ];
    run_cases(&cases).await;
}

/// Partition maintenance of every routine kind, and the routine operations of
/// slice 1's matrix applied to nodes, pause nothing.
fn maintenance_cases() -> Vec<Case> {
    let attach = "create table public.{t}_l4 (like public.{t} including defaults); \
                  alter table public.{t} attach partition public.{t}_l4 for values in (3)";
    let resplit = "alter table public.{t}_l2 detach partition public.{t}_l2h0; \
                   alter table public.{t}_l2 detach partition public.{t}_l2h1; \
                   create table public.{t}_l2n0 partition of public.{t}_l2 \
                       for values with (modulus 4, remainder 0); \
                   create table public.{t}_l2n1 partition of public.{t}_l2 \
                       for values with (modulus 4, remainder 1); \
                   create table public.{t}_l2n2 partition of public.{t}_l2 \
                       for values with (modulus 4, remainder 2); \
                   create table public.{t}_l2n3 partition of public.{t}_l2 \
                       for values with (modulus 4, remainder 3); \
                   insert into public.{t}_l2 select * from public.{t}_l2h0; \
                   insert into public.{t}_l2 select * from public.{t}_l2h1; \
                   drop table public.{t}_l2h0; drop table public.{t}_l2h1";
    vec![
        ("attach".into(), w(&[attach]), Expect::Nothing),
        (
            "detach".into(),
            w(&["alter table public.{t} detach partition public.{t}_l3"]),
            Expect::Nothing,
        ),
        (
            "detach_drop".into(),
            w(&[
                "alter table public.{t} detach partition public.{t}_l3",
                "drop table public.{t}_l3",
            ]),
            Expect::Nothing,
        ),
        (
            "default".into(),
            w(&["create table public.{t}_def partition of public.{t} default"]),
            Expect::Nothing,
        ),
        ("resplit".into(), w(&[resplit]), Expect::Nothing),
        (
            "recreated".into(),
            w(&["drop table public.{t}_l3; \
                 create table public.{t}_l3 partition of public.{t} for values in (2)"]),
            Expect::Nothing,
        ),
        (
            // pg_partman: a standalone table is built and attached; later the
            // oldest is detached and dropped.
            "partman".into(),
            w(&[
                "create table public.{t}_p4 (like public.{t} including all); \
                 alter table public.{t}_p4 add constraint {t}_p4_chk check (g = 4)",
                "alter table public.{t} attach partition public.{t}_p4 for values in (4)",
                "create table public.{t}_p5 (like public.{t} including all)",
                "alter table public.{t} attach partition public.{t}_p5 for values in (5)",
                "alter table public.{t} detach partition public.{t}_p4",
                "drop table public.{t}_p4",
            ]),
            Expect::Nothing,
        ),
        (
            // A detached leaf attached again, between two passes and across
            // several.
            "reattach".into(),
            w(&[
                "alter table public.{t} detach partition public.{t}_l3; \
                 alter table public.{t} attach partition public.{t}_l3 for values in (2)",
                "alter table public.{t} detach partition public.{t}_l3",
                "alter table public.{t} attach partition public.{t}_l3 for values in (2)",
            ]),
            Expect::Nothing,
        ),
        (
            // The operator removes the triggers of a leaf they detached.
            "detached_cleanup".into(),
            w(&[
                "alter table public.{t} detach partition public.{t}_l3",
                "drop trigger if exists x on public.{t}_l3; \
                 do $$ declare r record; begin \
                   for r in select tgname from pg_trigger \
                            where tgrelid = 'public.{t}_l3'::regclass \
                              and tgname like 'trellis_capture_%' \
                   loop execute format('drop trigger %I on public.{t}_l3', r.tgname); end loop; \
                 end $$",
                "alter table public.{t} attach partition public.{t}_l3 for values in (2)",
            ]),
            Expect::Nothing,
        ),
        (
            "node_maintenance".into(),
            w(&[
                "vacuum full public.{t}_l1",
                "cluster public.{t}_l1 using {t}_l1_pkey",
                "reindex table public.{t}",
                "analyze public.{t}",
                "vacuum (analyze) public.{t}_l2h0",
                "alter table public.{t} add column extra int; \
                 comment on table public.{t}_l1 is 'migrated'; \
                 grant select on public.{t} to public; \
                 alter table public.{t}_l1 set (fillfactor = 70)",
                "begin; alter table public.{t}_l1 rename to {t}_l1_away; \
                 alter table public.{t}_l1_away rename to {t}_l1; commit",
                "insert into public.{t} select i, 1, i, i, i from generate_series(100, 130) i; \
                 delete from public.{t} where id > 125; truncate public.{t}_l1",
                "vacuum freeze pg_catalog.pg_trigger",
                "vacuum full pg_catalog.pg_trigger",
            ]),
            Expect::Nothing,
        ),
        (
            // `pg_upgrade` recreates every trigger under a new oid: the rows of
            // the other database are re-recorded. A restore pauses every
            // definition of the database (`logical_restore.rs`,
            // `partition_restore.rs`), so it is not a case among the others.
            "pg_upgrade".into(),
            ["{t}", "{t}_l1", "{t}_l2", "{t}_l2h0", "{t}_l2h1", "{t}_l3"]
                .iter()
                .map(|rel| recreate_triggers(rel))
                .chain(["update capture_triggers set system_identifier = '1' \
                 where table_name = 'public.{t}'"
                    .to_string()])
                .collect::<Vec<_>>()
                .chunks(7)
                .map(|c| c.join("; "))
                .collect(),
            Expect::Nothing,
        ),
    ]
}

/// Part `i` of `n` of `cases` (every case lands in exactly one part).
fn part(cases: &[Case], i: usize, n: usize) -> &[Case] {
    let size = cases.len().div_ceil(n);
    &cases[(i * size).min(cases.len())..((i + 1) * size).min(cases.len())]
}

#[tokio::test]
async fn partition_maintenance_pauses_nothing_part_1() {
    run_cases(part(&maintenance_cases(), 0, 2)).await;
}

#[tokio::test]
async fn partition_maintenance_pauses_nothing_part_2() {
    run_cases(part(&maintenance_cases(), 1, 2)).await;
}

/// The rows `capture_triggers` holds for `table`, as `(nodes, rows)`.
async fn record_size(raw: &Client, table: &str) -> (i64, i64) {
    let row = raw
        .query_one(
            "select count(distinct node_oid), count(*) from capture_triggers \
             where table_name = $1",
            &[&table],
        )
        .await
        .expect("count the record");
    (row.get(0), row.get(1))
}

async fn capture_failures(trellis: &Trellis, names: &[&str]) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for name in names {
        let status = trellis.status(name).await.unwrap().expect("the reader");
        out.push(status.capture_failure.map(|f| f.error));
    }
    out
}

/// A tree's nodes are recorded as Trellis installs them, the nodes partition
/// maintenance adds included, so a disruption of a leaf that was added later
/// pauses too; a node that leaves the tree loses its rows.
#[tokio::test]
async fn nodes_added_by_maintenance_are_recorded_and_then_checked() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    create_case_tables(&raw, &trellis, &["m".to_string()]).await;
    bring_live(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let table = "public.p_m";
    assert_eq!(record_size(&raw, table).await, (6, 30));

    // A new leaf, a DEFAULT, a re-split: recorded by the install that adds
    // their triggers.
    app.batch_execute(
        "create table public.p_m_l4 partition of public.p_m for values in (3); \
         create table public.p_m_def partition of public.p_m default",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(record_size(&raw, table).await, (8, 40));
    assert_eq!(
        capture_failures(&trellis, &["r_m", "s_m"]).await,
        vec![None, None]
    );

    // A node that leaves the tree loses its rows.
    app.batch_execute("alter table public.p_m detach partition public.p_m_l4")
        .await
        .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(record_size(&raw, table).await, (7, 35));

    // The default leaf was added after the first install; disabling it pauses.
    app.batch_execute(&each_trigger(
        "p_m_def",
        "alter table public.p_m_def disable trigger %I",
    ))
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let failures = capture_failures(&trellis, &["r_m", "s_m"]).await;
    assert!(
        failures.iter().all(|f| f
            .as_deref()
            .is_some_and(|f| f.contains("(on public.p_m_def)"))),
        "{failures:?}"
    );
    // Nothing reinstalls while the readers are paused.
    let disabled = rows(
        &raw,
        "select count(*)::text from pg_trigger \
         where tgrelid = 'public.p_m_def'::regclass and tgname like 'trellis_capture_%' \
           and tgenabled = 'D'",
    )
    .await;
    assert_eq!(disabled, vec![vec![Some("5".to_string())]]);
}

/// A `RESUME` after a node's triggers were dropped reinstalls them and
/// records the whole tree afresh; the next passes pause nothing.
#[tokio::test]
async fn resume_reinstalls_and_records_a_disrupted_tree() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    create_case_tables(&raw, &trellis, &["rs".to_string()]).await;
    bring_live(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let table = "public.p_rs";

    app.batch_execute(&each_trigger(
        "p_rs_l2h1",
        "drop trigger %I on public.p_rs_l2h1",
    ))
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(statuses(&raw).await["s_rs"], TransformStatus::Paused);
    // The stale record stays while the readers are paused.
    assert_eq!(record_size(&raw, table).await, (6, 30));
    let none = rows(
        &raw,
        "select count(*)::text from pg_trigger \
         where tgrelid = 'public.p_rs_l2h1'::regclass and tgname like 'trellis_capture_%'",
    )
    .await;
    assert_eq!(none, vec![vec![Some("0".to_string())]]);

    for target in ["r_rs", "s_rs"] {
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .unwrap_or_else(|e| panic!("{target}: {e}"));
    }
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(record_size(&raw, table).await, (6, 30));
    assert_eq!(
        capture_failures(&trellis, &["r_rs", "s_rs"]).await,
        vec![None, None]
    );
    let back = rows(
        &raw,
        "select count(*)::text from pg_trigger \
         where tgrelid = 'public.p_rs_l2h1'::regclass and tgname like 'trellis_capture_%' \
           and tgenabled = 'A'",
    )
    .await;
    assert_eq!(back, vec![vec![Some("5".to_string())]]);
    // And the record is current: a quiet pass pauses nothing, a new
    // disruption pauses again.
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        capture_failures(&trellis, &["r_rs", "s_rs"]).await,
        vec![None, None]
    );
}

/// A tree of 100 tables or more installs root first and then one table per
/// transaction. A pass that stops between two of them (a leaf held under a
/// lock past the pass's deadline) leaves a record for exactly the tables that
/// carry triggers: the next passes pause nothing, finish the install, and
/// record the rest.
#[tokio::test]
async fn an_install_that_stops_between_transactions_pauses_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    let leaves = 110;
    let mut ddl = String::from(
        "create table public.big (id int not null, g int not null, v int, \
             primary key (g, id)) partition by list (g);",
    );
    for i in 1..=leaves {
        ddl.push_str(&format!(
            "create table public.big_l{i:03} partition of public.big for values in ({i});"
        ));
    }
    ddl.push_str("insert into public.big select i, i % 100 + 1, i from generate_series(1, 500) i");
    raw.batch_execute(&ddl).await.unwrap();
    trellis
        .apply("TRANSFORM s_big FROM public.big GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(v) AS total")
        .await
        .unwrap();

    // Another session is mid-write on one leaf (a row lock mode that blocks
    // the install's table lock), so the install stops at it.
    let holder = connect(db.dsn()).await;
    holder
        .batch_execute("begin; lock table public.big_l050 in row exclusive mode")
        .await
        .unwrap();
    let desired = trellis::defs::tables_to_capture(&db.pool).await.unwrap();
    let mut stopped = false;
    for _ in 0..3 {
        let outcome = reconcile::reconcile(
            &mut raw,
            SCHEMA,
            &desired,
            Instant::now() + Duration::from_millis(1500),
        )
        .await
        .expect("capture pass");
        stopped |= !outcome.waiting.is_empty();
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
        assert_eq!(
            capture_failures(&trellis, &["s_big"]).await,
            vec![None],
            "a half-finished install is not a disruption"
        );
    }
    assert!(stopped, "the held leaf stops the install");
    let (nodes, count) = record_size(&raw, "public.big").await;
    assert!(
        (2..=leaves).contains(&nodes) && count == nodes * 5,
        "the root and the leaves before the held one: {nodes} nodes, {count} rows"
    );
    let carrying = rows(
        &raw,
        "select count(distinct tgrelid)::text from pg_trigger \
         where tgname like 'trellis_capture_%' and tgrelid in \
           (select oid from pg_class where relname like 'big%')",
    )
    .await;
    assert_eq!(carrying[0][0].clone().unwrap(), nodes.to_string());

    holder.batch_execute("rollback").await.unwrap();
    for _ in 0..2 {
        capture_pass(&mut raw, &db.pool).await;
    }
    assert_eq!(
        record_size(&raw, "public.big").await,
        (leaves + 1, (leaves + 1) * 5)
    );
    assert_eq!(capture_failures(&trellis, &["s_big"]).await, vec![None]);

    // The finished tree is checked: a leaf disrupted now pauses the reader.
    raw.batch_execute(&each_trigger(
        "big_l050",
        "alter table public.big_l050 disable trigger %I",
    ))
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    let failures = capture_failures(&trellis, &["s_big"]).await;
    assert!(
        failures[0]
            .as_deref()
            .is_some_and(|f| f.contains("(on public.big_l050)")),
        "{failures:?}"
    );
}

/// One root case: its id, the windows that disrupt the root (a capture pass
/// after each), the `capture_failure` text both readers must carry, and the
/// windows that put a table at the name for the resume to re-validate.
type RootCase = (&'static str, Vec<String>, &'static str, Vec<String>);

async fn count(raw: &Client, sql: &str) -> String {
    rows(raw, sql).await[0][0].clone().unwrap()
}

/// A root that is dropped, renamed away, moved to another schema, swapped by
/// renames or dropped and recreated is table maintenance, not partition
/// maintenance (#1200): both readers pause on the first pass that sees it,
/// nothing is corrected and nothing is reinstalled before a `RESUME`, and
/// after it the targets equal what the source holds.
#[tokio::test]
async fn a_root_swapped_recreated_or_renamed_away_pauses_both_readers_until_resumed() {
    let cases: Vec<RootCase> = vec![
        (
            "renamed",
            w(&["alter table public.{t} rename to {t}_away"]),
            "no longer exists",
            w(&["alter table public.{t}_away rename to {t}"]),
        ),
        (
            "schema",
            w(&["create schema if not exists elsewhere; \
                 alter table public.{t} set schema elsewhere"]),
            "no longer exists",
            w(&["alter table elsewhere.{t} set schema public"]),
        ),
        (
            "swapped",
            vec![format!(
                "{} insert into public.{{t}}_n select i + 100, i % 3, i, i * 2, i \
                    from generate_series(1, 12) i; \
                 alter table public.{{t}} rename to {{t}}_old; \
                 alter table public.{{t}}_n rename to {{t}}",
                tree_ddl("{t}_n")
            )],
            "not the one Trellis captured",
            vec![],
        ),
        (
            "empty",
            w(&["drop table public.{t}; \
                 create table public.{t} (id int not null, g int not null, a int, b int, \
                    u int, primary key (g, id)) partition by list (g)"]),
            "not the one Trellis captured",
            vec![],
        ),
        (
            "leaves",
            vec![format!(
                "drop table public.{{t}}; {}",
                tree_ddl("{t}").replace("generate_series(1, 30)", "generate_series(1, 9)")
            )],
            "not the one Trellis captured",
            vec![],
        ),
    ];
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    let ids: Vec<String> = cases.iter().map(|c| c.0.to_string()).collect();
    create_case_tables(&raw, &trellis, &ids).await;
    bring_live(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    let mut failures = Vec::new();
    for (id, windows, text, restore) in &cases {
        let t = format!("p_{id}");
        for window in windows {
            app.batch_execute(&window.replace("{t}", &t))
                .await
                .unwrap_or_else(|e| panic!("{id}: {window}: {e:?}"));
            capture_pass(&mut raw, &db.pool).await;
        }
        let now = settle(&mut raw, &db.pool, &trellis, id, &mut failures).await;
        let ok = now.iter().all(|(s, f)| {
            *s == TransformStatus::Paused && f.as_deref().is_some_and(|f| f.contains(text))
        });
        if !ok {
            failures.push(format!("{id}: expected a pause naming {text:?}: {now:?}"));
        }
        let queued = count(
            &raw,
            &format!(
                "select count(*)::text from partition_corrections where table_name = 'public.{t}'"
            ),
        )
        .await;
        if queued != "0" {
            failures.push(format!("{id}: {queued} corrections queued"));
        }
        let triggers = count(
            &raw,
            &format!(
                "select count(*)::text from pg_trigger where tgname like 'trellis_capture_%' \
                   and tgrelid = coalesce(to_regclass('public.{t}'), 0::oid) \
                   and tgrelid <> (select root_oid::oid from partition_sources \
                                   where table_name = 'public.{t}')"
            ),
        )
        .await;
        if triggers != "0" {
            failures.push(format!("{id}: capture reinstalled on the new root"));
        }
        for window in restore {
            app.batch_execute(&window.replace("{t}", &t))
                .await
                .unwrap_or_else(|e| panic!("{id}: {window}: {e:?}"));
        }
        for reader in ["r", "s"] {
            trellis
                .apply(&format!("RESUME TRANSFORM {reader}_{id}"))
                .await
                .unwrap_or_else(|e| panic!("{id}: resume {reader}: {e}"));
        }
    }
    bring_live(&mut raw, &db.pool).await;
    for (id, ..) in &cases {
        let t = format!("p_{id}");
        let target = rows(
            &raw,
            &format!("select g::text, total::text from public.s_{id} order by 1"),
        )
        .await;
        let source = rows(
            &raw,
            &format!("select g::text, sum(b)::text from public.{t} group by g order by 1"),
        )
        .await;
        if target != source {
            failures.push(format!("{id}: sum {target:?} != {source:?}"));
        }
        let target = rows(
            &raw,
            &format!("select g::text, a::text, n::text from public.r_{id} order by 1, 2"),
        )
        .await;
        let source = rows(
            &raw,
            &format!("select g::text, a::text, count(*)::text from public.{t} group by g, a order by 1, 2"),
        )
        .await;
        if target != source {
            failures.push(format!("{id}: count {target:?} != {source:?}"));
        }
    }
    capture_pass(&mut raw, &db.pool).await;
    for (t, s) in statuses(&raw).await {
        if s != TransformStatus::Live {
            failures.push(format!("{t} is {s:?} after the resume"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A root swapped by renames after the pass's check that the name still holds
/// the recorded root, and before its deep check reads the tree, pauses both
/// readers in that same pass: the new root is never recorded as the source's,
/// so the next pass can't take it for the one Trellis captured.
#[tokio::test]
async fn a_root_swapped_during_the_pass_pauses_both_readers() {
    use trellis::staging::interleave::{PausePoint, PauseScope, with_scope};
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    create_case_tables(&raw, &trellis, &["race".to_string()]).await;
    bring_live(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let gate = connect(db.dsn()).await;
    let recorded_root = || async {
        count(
            &raw,
            "select root_oid::text from partition_sources where table_name = 'public.p_race'",
        )
        .await
    };
    let old_root = recorded_root().await;

    const LOCK: i64 = 1_200_001;
    gate.execute("select pg_advisory_lock($1)", &[&LOCK])
        .await
        .unwrap();
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::BeforeFingerprintRead, "public.p_race", LOCK);
    let dsn = db.dsn().to_string();
    let pool = db.pool.clone();
    let pass = tokio::spawn(with_scope(scope, async move {
        let mut client = connect(&dsn).await;
        let desired = trellis::defs::tables_to_capture(&pool).await.unwrap();
        reconcile::reconcile(
            &mut client,
            SCHEMA,
            &desired,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("capture pass")
    }));
    reached.await.expect("the pass reaches the pause");
    app.batch_execute(&format!(
        "{} alter table public.p_race rename to p_race_old; \
         alter table public.p_race_n rename to p_race",
        tree_ddl("p_race_n")
    ))
    .await
    .unwrap();
    gate.execute("select pg_advisory_unlock($1)", &[&LOCK])
        .await
        .unwrap();
    let outcome = pass.await.unwrap();
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);

    let failures = capture_failures(&trellis, &["r_race", "s_race"]).await;
    assert!(
        failures.iter().all(|f| f
            .as_deref()
            .is_some_and(|f| f.contains("not the one Trellis captured"))),
        "{failures:?}"
    );
    assert_eq!(
        recorded_root().await,
        old_root,
        "the new root isn't recorded"
    );
    let queued = count(
        &raw,
        "select count(*)::text from partition_corrections where table_name = 'public.p_race'",
    )
    .await;
    assert_eq!(queued, "0");
}

/// Both of case `id`'s targets equal what its source holds.
async fn oracle(raw: &Client, id: &str, failures: &mut Vec<String>) {
    let t = format!("p_{id}");
    for (target, source) in [
        (
            format!("select g::text, total::text from public.s_{id} order by 1"),
            format!("select g::text, sum(b)::text from public.{t} group by g order by 1"),
        ),
        (
            format!("select g::text, a::text, n::text from public.r_{id} order by 1, 2"),
            format!(
                "select g::text, a::text, count(*)::text from public.{t} group by g, a order by 1, 2"
            ),
        ),
    ] {
        let (target, source) = (rows(raw, &target).await, rows(raw, &source).await);
        if target != source {
            failures.push(format!("{id}: {target:?} != {source:?}"));
        }
    }
}

/// A source whose every reader is paused for a capture failure records its
/// tree and queues nothing, so the resume's rebuild must cover what partition
/// maintenance and writes did meanwhile: a node's triggers disabled, or the
/// root renamed away and back, then a leaf attached with rows, one detached,
/// and rows written, updated and deleted before the `RESUME`.
#[tokio::test]
async fn tree_changes_while_every_reader_is_paused_are_covered_by_the_resume() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    let ids = ["node".to_string(), "away".to_string()];
    create_case_tables(&raw, &trellis, &ids).await;
    bring_live(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    // `{t}` the root's name now, `{l}` its leaves' prefix.
    let maintenance = "create table public.{l}_l4 (like public.{t} including defaults); \
         insert into public.{l}_l4 select i + 500, 3, i, i, i from generate_series(1, 7) i; \
         alter table public.{t} attach partition public.{l}_l4 for values in (3); \
         alter table public.{t} detach partition public.{l}_l3; \
         insert into public.{t} select i + 900, i % 2, i, i * 5, i from generate_series(1, 9) i; \
         update public.{t} set b = b + 1000 where g = 0; \
         delete from public.{t} where id < 4";
    app.batch_execute(&each_trigger(
        "p_node_l1",
        "alter table public.p_node_l1 disable trigger %I",
    ))
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    app.batch_execute(
        &maintenance
            .replace("{l}", "p_node")
            .replace("{t}", "p_node"),
    )
    .await
    .unwrap();
    app.batch_execute("alter table public.p_away rename to p_away_x")
        .await
        .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    app.batch_execute(
        &maintenance
            .replace("{l}", "p_away")
            .replace("{t}", "p_away_x"),
    )
    .await
    .unwrap();
    app.batch_execute("alter table public.p_away_x rename to p_away")
        .await
        .unwrap();
    for _ in 0..3 {
        full_pass(&mut raw, &db.pool).await;
        run_backfill_chunks(&db.pool).await;
        drain_to_quiescence(&db.pool, &mut raw).await;
    }
    for (t, s) in statuses(&raw).await {
        assert_eq!(s, TransformStatus::Paused, "{t}");
    }
    for id in &ids {
        let queued = count(
            &raw,
            &format!(
                "select count(*)::text from partition_corrections where table_name = 'public.p_{id}'"
            ),
        )
        .await;
        assert_eq!(
            queued, "0",
            "{id}: nothing is queued while every reader is paused"
        );
        for reader in ["r", "s"] {
            trellis
                .apply(&format!("RESUME TRANSFORM {reader}_{id}"))
                .await
                .unwrap_or_else(|e| panic!("{id}: {e}"));
        }
        // Written before any pass sees the resume, and on the new leaf after.
        app.batch_execute(&format!(
            "insert into public.p_{id} select i + 2000, 3, i, i, i from generate_series(1, 4) i"
        ))
        .await
        .unwrap();
    }
    bring_live(&mut raw, &db.pool).await;
    for id in &ids {
        app.batch_execute(&format!(
            "insert into public.p_{id}_l4 select i + 3000, 3, i, i, i from generate_series(1, 4) i"
        ))
        .await
        .unwrap();
    }
    bring_live(&mut raw, &db.pool).await;
    let mut failures = Vec::new();
    for id in &ids {
        oracle(&raw, id, &mut failures).await;
    }
    assert!(failures.is_empty(), "{failures:#?}");
}
