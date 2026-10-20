//! Capture triggers or functions changed outside Trellis, and a read table
//! that is gone (#707, #999): the capture pass pauses every reader of the
//! table with a `capture_failure` naming what it found, nothing reinstalls
//! or rebuilds by itself, and routine maintenance and Trellis's own installs
//! pause nothing.
//!
//! Each case runs on a table of its own with two readers: `r_<case>`, a 1-1
//! passthrough, and `s_<case>`, `SUM(b)` by `g`. The case's windows run
//! between calls of the capture pass, which is called directly. Nothing
//! polls for convergence (#297).

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

/// One whole staging-worker pass: capture, markers, discharge.
async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(
        raw,
        pool,
        SCHEMA,
        "capture_disruption_wake",
        Duration::from_secs(2),
    )
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
    use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
    let watermark = StagedWatermark::saturated();
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
            &watermark,
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
    use trellis::defs::chunk_queue;
    const CLAIMED_BY: &str = "capture_disruption_backfill";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            trellis::staging::build::settle_builds(pool).await;
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                CLAIMED_BY,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
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
    /// Both `live`, nothing rebuilt.
    Nothing,
    /// Both paused, with a `capture_failure` containing this text.
    Paused(&'static str),
}

/// One case: its id, its windows (one batch of statements each, `{t}` the
/// case's table, a capture pass after each) and what the pass should do.
type Case = (&'static str, Vec<String>, Expect);

/// A `DO` block running `action` (with `%I` for the trigger name) on each of
/// the case table's Trellis capture triggers.
fn each_trigger(action: &str) -> String {
    format!(
        "do $$ declare r record; begin \
           for r in select tgname from pg_trigger \
                    where tgrelid = 'public.{{t}}'::regclass and tgname like 'trellis_capture_%' \
           loop execute format('{action}', r.tgname); end loop; end $$"
    )
}

/// A `DO` block that drops and creates each capture trigger of `public.<t>`
/// again, `ENABLE ALWAYS`: new oids, the same definitions.
fn recreate_triggers(t: &str) -> String {
    format!(
        "do $$ declare r record; begin \
           for r in select tgname, pg_get_triggerdef(oid) as def from pg_trigger \
                    where tgrelid = 'public.{t}'::regclass and tgname like 'trellis_capture_%' \
           loop execute format('drop trigger %I on public.{t}', r.tgname); \
                execute r.def; \
                execute format('alter table public.{t} enable always trigger %I', r.tgname); \
           end loop; end $$"
    )
}

fn w(windows: &[&str]) -> Vec<String> {
    windows.iter().map(|w| w.to_string()).collect()
}

async fn trigger_rows(raw: &Client) -> Vec<Vec<Option<String>>> {
    rows(
        raw,
        "select count(*)::text from capture_triggers where table_name = 'public.t_own'",
    )
    .await
}

async fn create_case_tables(raw: &Client, trellis: &Trellis, ids: &[String]) {
    for id in ids {
        let t = format!("t_{id}");
        raw.batch_execute(&format!(
            "create table public.{t} (id int primary key, g int not null, a int, b int, u int); \
             insert into public.{t} select i, i % 3, i, i, i from generate_series(1, 30) i;"
        ))
        .await
        .expect("create the case's table");
        for text in [
            format!("TRANSFORM r_{id} FROM public.{t} SELECT a AS a, b AS b"),
            format!("TRANSFORM s_{id} FROM public.{t} GROUP BY g SELECT g AS g, SUM(b) AS total"),
        ] {
            trellis.apply(&text).await.expect(&text);
        }
    }
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
        let t = format!("t_{id}");
        for window in windows {
            app.batch_execute(&window.replace("{t}", &t))
                .await
                .unwrap_or_else(|e| panic!("{id}: {window}: {e:?}"));
            capture_pass(&mut raw, &db.pool).await;
        }
        let mut reasons = Vec::new();
        for _ in 0..2 {
            capture_pass(&mut raw, &db.pool).await;
            let mut now = Vec::new();
            for reader in ["r", "s"] {
                let status = trellis.status(&format!("{reader}_{id}")).await.unwrap();
                let status = status.expect("the reader exists");
                now.push((status.status, status.capture_failure.map(|f| f.error)));
            }
            reasons.push(now);
        }
        // A second pass never changes the reason.
        if reasons[0] != reasons[1] {
            failures.push(format!("{id}: the reason changed: {reasons:?}"));
        }
        let now = &reasons[1];
        match expect {
            Expect::Nothing => {
                if now
                    .iter()
                    .any(|(s, f)| *s != TransformStatus::Live || f.is_some())
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

/// Every case of #707 rule 2 pauses on the first pass that sees it.
#[tokio::test]
async fn disruption_of_capture_pauses_the_readers_with_the_cause() {
    let recreate_by_hand = recreate_triggers("{t}");
    let replace_function = "do $$ declare r record; begin \
           for r in select distinct tgfoid::regproc::text as f from pg_trigger \
                    where tgrelid = 'public.{t}'::regclass and tgname like 'trellis_capture_%' \
           loop execute format('create or replace function %s() returns trigger \
                language plpgsql security definer as $b$ begin return null; end $b$', r.f); \
           end loop; end $$"
        .to_string();
    let swap = "create table public.{t}_new (like public.{t} including all); \
                alter table public.{t} rename to {t}_old; \
                alter table public.{t}_new rename to {t}"
        .to_string();
    let drop_and_recreate = "create table public.{t}_new (like public.{t} including all); \
                             drop table public.{t}; alter table public.{t}_new rename to {t}"
        .to_string();
    let origin = "alter table public.{t} disable trigger user; \
                  alter table public.{t} enable trigger user"
        .to_string();
    let set_schema = "create schema if not exists elsewhere; \
                      alter table public.{t} set schema elsewhere"
        .to_string();
    run_cases(&[
        (
            "D1",
            vec!["drop table public.{t}".into()],
            Expect::Paused("no longer exists"),
        ),
        (
            "D2",
            vec!["alter table public.{t} rename to {t}_away".into()],
            Expect::Paused("no longer exists"),
        ),
        ("D3", vec![set_schema], Expect::Paused("no longer exists")),
        ("D4", vec![swap], Expect::Paused("is gone")),
        ("D5", vec![drop_and_recreate], Expect::Paused("is gone")),
        (
            "D6",
            vec![each_trigger("drop trigger %I on public.{t}")],
            Expect::Paused("is gone"),
        ),
        (
            "D7",
            vec![recreate_by_hand],
            Expect::Paused("dropped and created again"),
        ),
        (
            "D8",
            vec![each_trigger("alter table public.{t} disable trigger %I")],
            Expect::Paused("is disabled"),
        ),
        ("D9", vec![origin], Expect::Paused("ORIGIN mode")),
        (
            "D10",
            vec![each_trigger(
                "alter table public.{t} enable replica trigger %I",
            )],
            Expect::Paused("REPLICA mode"),
        ),
        (
            "D11",
            vec![replace_function],
            Expect::Paused("was replaced"),
        ),
        (
            "D13",
            vec![
                "alter table public.{t} rename to {t}_v2; \
                  create view public.{t} as select * from public.{t}_v2"
                    .into(),
            ],
            Expect::Paused("no longer exists"),
        ),
        (
            "D12",
            vec![each_trigger(
                "alter table public.{t} disable trigger %1$I; \
                 alter table public.{t} enable always trigger %1$I",
            )],
            Expect::Paused("was altered"),
        ),
    ])
    .await;
}

/// The routine operations of #999's matrix, and a change to the table that
/// leaves Trellis's triggers and functions untouched, pause nothing.
#[tokio::test]
async fn routine_maintenance_and_migrations_pause_nothing() {
    run_cases(&[
        ("N1", vec!["vacuum full public.{t}".into()], Expect::Nothing),
        (
            "N2",
            vec!["cluster public.{t} using {t}_pkey".into()],
            Expect::Nothing,
        ),
        (
            "N3",
            vec!["reindex table public.{t}".into()],
            Expect::Nothing,
        ),
        (
            "N4",
            vec![
                "analyze public.{t}".into(),
                "vacuum (analyze) public.{t}".into(),
            ],
            Expect::Nothing,
        ),
        (
            "N5",
            vec![
                "vacuum freeze pg_catalog.pg_trigger".into(),
                "vacuum full pg_catalog.pg_trigger".into(),
            ],
            Expect::Nothing,
        ),
        (
            "N6",
            vec![
                "vacuum freeze pg_catalog.pg_proc".into(),
                "vacuum freeze pg_catalog.pg_description".into(),
            ],
            Expect::Nothing,
        ),
        (
            "N7",
            w(&["begin; alter table public.{t} rename to {t}_away; \
               alter table public.{t}_away rename to {t}; commit"]),
            Expect::Nothing,
        ),
        (
            "N8",
            w(&["alter table public.{t} add column extra int; \
               create index {t}_a on public.{t} (a); \
               alter table public.{t} set (fillfactor = 70); \
               comment on table public.{t} is 'migrated'; \
               alter table public.{t} owner to current_user; \
               grant select on public.{t} to public"]),
            Expect::Nothing,
        ),
        (
            "N9",
            w(&[
                "create function public.{t}_noop() returns trigger language plpgsql \
                 as $f$ begin return null; end $f$; \
               create trigger {t}_app after insert on public.{t} \
                 for each statement execute function public.{t}_noop(); \
               drop trigger {t}_app on public.{t}",
            ]),
            Expect::Nothing,
        ),
        (
            "N10",
            w(&[
                "insert into public.{t} select i, 1, i, i, i from generate_series(100, 130) i; \
               delete from public.{t} where id > 125; truncate public.{t}",
            ]),
            Expect::Nothing,
        ),
    ])
    .await;
}

/// The record of a restored database is re-recorded instead of pausing:
/// another database's `system_identifier` or `capture_triggers` oid, with
/// triggers that have new oids. Mutation check: without the restore check,
/// the second case pauses.
#[tokio::test]
async fn a_restored_record_is_re_recorded_not_acted_on() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    let ids = vec!["rs1".to_string(), "rs2".to_string()];
    create_case_tables(&raw, &trellis, &ids).await;
    bring_live(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    // A dump and restore: another oid for the catalog table.
    app.batch_execute(&recreate_triggers("t_rs1"))
        .await
        .unwrap();
    app.batch_execute(
        "update capture_triggers set catalog_oid = (catalog_oid::bigint + 1000)::oid \
                       where table_name = 'public.t_rs1'",
    )
    .await
    .unwrap();
    // `pg_upgrade`: another cluster identifier, with every trigger recreated.
    app.batch_execute(&recreate_triggers("t_rs2"))
        .await
        .unwrap();
    app.batch_execute(
        "update capture_triggers set system_identifier = '1' \
                       where table_name = 'public.t_rs2'",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    for (t, s) in statuses(&raw).await {
        assert_eq!(s, TransformStatus::Live, "{t}");
    }
    // Re-recorded with the live oids.
    let stale = rows(
        &raw,
        "select c.table_name from capture_triggers c \
         join pg_trigger g on g.tgname = c.trigger_name \
          and g.tgrelid = c.table_name::regclass \
         where g.oid <> c.trigger_oid or c.system_identifier = '1' \
            or c.catalog_oid <> 'capture_triggers'::regclass::oid",
    )
    .await;
    assert!(stale.is_empty(), "{stale:?}");
    // And now that they are recorded, a recreation is a disruption again.
    app.batch_execute(&recreate_triggers("t_rs2"))
        .await
        .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(statuses(&raw).await["r_rs2"], TransformStatus::Paused);
}

/// Trellis's own installs, widens, narrows and uninstalls pause nothing.
#[tokio::test]
async fn trellis_s_own_capture_changes_pause_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    let ids = vec!["own".to_string()];
    create_case_tables(&raw, &trellis, &ids).await;
    bring_live(&mut raw, &db.pool).await;
    assert_eq!(trigger_rows(&raw).await, vec![vec![Some("5".to_string())]]);

    // A widen: a new reader needs a column the images lack.
    trellis
        .apply("TRANSFORM w_own FROM public.t_own SELECT u AS u")
        .await
        .unwrap();
    bring_live(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    // A narrow: that reader goes.
    trellis.apply("PAUSE TRANSFORM w_own").await.unwrap();
    trellis.apply("DROP TRANSFORM w_own").await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    // An uninstall, and a first install again.
    for t in ["r_own", "s_own"] {
        trellis
            .apply(&format!("PAUSE TRANSFORM {t}"))
            .await
            .unwrap();
        trellis.apply(&format!("DROP TRANSFORM {t}")).await.unwrap();
    }
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(trigger_rows(&raw).await, vec![vec![Some("0".to_string())]]);
    trellis
        .apply("TRANSFORM again FROM public.t_own SELECT a AS a")
        .await
        .unwrap();
    bring_live(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(trigger_rows(&raw).await, vec![vec![Some("5".to_string())]]);
    for (t, s) in statuses(&raw).await {
        assert_eq!(s, TransformStatus::Live, "{t}");
    }
}

/// A function body an older Trellis left (it still matches its own recorded
/// hash) is replaced by a plain widen: no pause. A hand edit is
/// `disruption_of_capture_pauses_the_readers_with_the_cause`'s D11.
#[tokio::test]
async fn a_stale_generated_body_widens_without_a_pause() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    create_case_tables(&raw, &trellis, &["old".to_string()]).await;
    bring_live(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    // An older generator's body: other text, with a comment whose hash
    // names that text.
    app.batch_execute(
        "do $$ declare r record; h text; begin \
           for r in select distinct tgfoid as f from pg_trigger \
                    where tgrelid = 'public.t_old'::regclass and tgname like 'trellis_capture_%' \
           loop \
             execute format('create or replace function %s() returns trigger \
                language plpgsql security definer as $b$ begin return null; end $b$', r.f::regproc); \
           end loop; end $$",
    )
    .await
    .unwrap();
    // Re-point each comment's hash at the new text, as the old generator
    // would have recorded.
    let functions = rows(
        &raw,
        "select distinct tgfoid::regproc::text from pg_trigger \
         where tgrelid = 'public.t_old'::regclass and tgname like 'trellis_capture_%'",
    )
    .await;
    for f in functions {
        let f = f[0].clone().unwrap();
        let hash = trellis::capture::sql::body_hash(" begin return null; end ");
        let comment = rows(
            &raw,
            &format!("select obj_description('{f}'::regproc, 'pg_proc')"),
        )
        .await[0][0]
            .clone()
            .unwrap();
        let (head, _) = comment.rsplit_once(",\"body\":").unwrap();
        app.batch_execute(&format!(
            "comment on function {f}() is '{head},\"body\":\"{hash}\"}}'"
        ))
        .await
        .unwrap();
    }
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    for (t, s) in statuses(&raw).await {
        assert_eq!(s, TransformStatus::Live, "{t}");
    }
    // The widen put the generated body back.
    let body = rows(
        &raw,
        "select count(*)::text from pg_proc where length(prosrc) < 100 \
         and proname like 'cap_%_t_old_%'",
    )
    .await;
    assert_eq!(body, vec![vec![Some("0".to_string())]]);
}

/// A `RESUME` refuses while the table is missing, and after a swap it
/// re-validates, reinstalls and records capture again, and rebuilds the
/// readers to what the table holds; a swapped-in table define would refuse
/// makes it refuse.
#[tokio::test]
async fn resume_waits_for_the_table_and_rebuilds_after_a_swap() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    create_case_tables(&raw, &trellis, &["sw".to_string()]).await;
    bring_live(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute("alter table public.t_sw rename to t_sw_away")
        .await
        .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(statuses(&raw).await["r_sw"], TransformStatus::Paused);
    let refused = trellis.apply("RESUME TRANSFORM r_sw").await;
    assert!(
        refused.is_err(),
        "a resume refuses while the table is missing"
    );
    assert_eq!(statuses(&raw).await["r_sw"], TransformStatus::Paused);

    // A swapped-in table define would refuse: no primary key.
    app.batch_execute("create table public.t_sw (id int, g int not null, a int, b int, u int)")
        .await
        .unwrap();
    let refused = trellis.apply("RESUME TRANSFORM r_sw").await;
    assert!(
        refused.is_err(),
        "a table define would refuse refuses the resume"
    );
    app.batch_execute("drop table public.t_sw").await.unwrap();

    // The swap completes: a table with other rows under the name.
    app.batch_execute(
        "create table public.t_sw (id int primary key, g int not null, a int, b int, u int); \
         insert into public.t_sw select i, i % 4, i * 2, i * 3, i from generate_series(1, 40) i",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(statuses(&raw).await["s_sw"], TransformStatus::Paused);
    for target in ["r_sw", "s_sw"] {
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .unwrap_or_else(|e| panic!("{target}: {e}"));
    }
    bring_live(&mut raw, &db.pool).await;
    assert_eq!(
        rows(
            &raw,
            "select a::text, b::text from public.r_sw order by 1, 2"
        )
        .await,
        rows(
            &raw,
            "select (i * 2)::text, (i * 3)::text from generate_series(1, 40) i order by 1, 2"
        )
        .await
    );
    assert_eq!(
        rows(
            &raw,
            "select g::text, total::text from public.s_sw order by 1"
        )
        .await,
        rows(
            &raw,
            "select (i % 4)::text, sum(i * 3)::text from generate_series(1, 40) i \
             group by i % 4 order by 1"
        )
        .await
    );
    // Capture is installed and recorded again, and a quiet pass pauses nothing.
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        rows(
            &raw,
            "select count(*)::text from capture_triggers where table_name = 'public.t_sw'"
        )
        .await,
        vec![vec![Some("5".to_string())]]
    );
    for (t, s) in statuses(&raw).await {
        assert_eq!(s, TransformStatus::Live, "{t}");
    }
}

/// A real `pg_dump` and `pg_restore` of the whole database, into another
/// database and back over the same one with `--clean`, recreates every
/// capture trigger under a new oid and pauses nothing: the restored record
/// names another `capture_triggers` oid, so the pass records afresh. A
/// restore of the application's schema alone recreates the triggers with
/// Trellis's record untouched, which is a recreation: it pauses.
#[tokio::test]
async fn a_real_dump_and_restore_pauses_nothing() {
    use std::process::Command;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    create_case_tables(&raw, &trellis, &["rr".to_string()]).await;
    bring_live(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let dump = cluster.root().join("whole.dump");
    let run = |command: &mut Command| {
        let out = command.output().expect("run a client tool");
        assert!(
            out.status.success(),
            "{command:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run(Command::new("pg_dump")
        .args(["-Fc", "-f"])
        .arg(&dump)
        .arg(db.dsn()));

    // Into another database of the cluster.
    let other = cluster.create_empty_database().await;
    run(Command::new("pg_restore")
        .args(["--no-owner", "-d", other.dsn()])
        .arg(&dump));
    let mut restored = connect(other.dsn()).await;
    capture_pass(&mut restored, &other.pool).await;
    capture_pass(&mut restored, &other.pool).await;
    for (t, s) in statuses(&restored).await {
        assert_eq!(
            s,
            TransformStatus::Live,
            "restored into another database: {t}"
        );
    }

    // Back over the same database.
    run(Command::new("pg_restore")
        .args(["--clean", "--if-exists", "--no-owner", "-d", db.dsn()])
        .arg(&dump));
    let mut raw = connect(db.dsn()).await;
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    for (t, s) in statuses(&raw).await {
        assert_eq!(s, TransformStatus::Live, "restored over itself: {t}");
    }

    // The application's schema alone.
    run(Command::new("pg_restore")
        .args([
            "--clean",
            "--if-exists",
            "--no-owner",
            "-n",
            "public",
            "-d",
            db.dsn(),
        ])
        .arg(&dump));
    capture_pass(&mut raw, &db.pool).await;
    for (t, s) in statuses(&raw).await {
        assert_eq!(
            s,
            TransformStatus::Paused,
            "the application schema alone: {t}"
        );
    }
}
