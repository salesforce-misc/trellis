//! `Trellis::status` of a 1-1 `JOIN` transform (ADR-0021, #1402): the sizes of
//! its hidden tables `R`, `M` and `J`, the step its build runs, and the
//! warning when one join key holds more `a` rows than the threshold. Warnings
//! only: the definition's status is unaffected. The sizes are the planner's
//! estimates, made current here by an explicit `ANALYZE`, so every number is
//! asserted exactly.
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`
//! and never polls for convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::dev::defs::set_one_to_one_join_fan_out_warning_rows;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, Step, WorkerOptions};
use trellis::{JoinBuildStep, JoinStatus, OneToOneBuildStep, Trellis, TrellisOptions};

const DDL: &str = "create table public.a (id text primary key, k integer, r integer, s text); \
                   create table public.b (bid bigint primary key, k integer, img integer, \
                       k0 text); \
                   create unique index on public.b (k); \
                   create index on public.a (k)";

/// Twenty `a` rows on keys 0 to 60 (every fifth on `NULL`), six `b` rows on
/// keys 10 to 60. `a`'s keys: 0 (2 rows), 10 (2), 20 (3), 30 (2), 40 (3),
/// 50 (2), 60 (2), and four `NULL`.
const SEED: &str = "insert into public.a \
                        select 'a' || lpad(i::text, 2, '0'), \
                               case when i % 5 = 0 then null else (i % 7) * 10 end, \
                               i, 's' || i \
                        from generate_series(1, 20) i; \
                    insert into public.b select i, i * 10, i * 100, 'k' || i \
                        from generate_series(1, 6) i";

const DEFINITION: &str = "TRANSFORM jl FROM public.a LEFT JOIN public.b ON a.k = b.k \
                          SELECT a.r + COALESCE(b.img, 0) AS total, b.k0 AS k0";

/// The dev knob and the warning threshold are process-wide.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn columns() -> Vec<(&'static str, ValueType)> {
    vec![
        ("id", ValueType::Text),
        ("k", ValueType::Integer(IntWidth::Int4)),
        ("r", ValueType::Integer(IntWidth::Int4)),
        ("s", ValueType::Text),
    ]
}

async fn start(built: bool) -> Driver {
    set_enabled(true);
    let ddl = format!("{DDL}; {SEED}");
    if built {
        Driver::start(&ddl, &columns(), &[DEFINITION], &["public.a", "public.b"]).await
    } else {
        Driver::start_unbuilt(
            &[],
            &ddl,
            &columns(),
            &[DEFINITION],
            &["public.a", "public.b"],
        )
        .await
    }
}

async fn trellis(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

async fn join_status(d: &Driver) -> JoinStatus {
    trellis(d)
        .await
        .status("jl")
        .await
        .expect("status")
        .expect("the definition exists")
        .join
        .expect("a JOIN reports its join status")
}

async fn step(d: &mut Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

async fn analyze(d: &Driver) {
    let id: i64 = d
        .ctl
        .query_one(
            "select id from transform_definitions where target_table = 'public.jl'",
            &[],
        )
        .await
        .expect("id")
        .get(0);
    d.ctl
        .batch_execute(&format!(
            "analyze {DEFAULT_SCHEMA}.join1_{id}_r; analyze {DEFAULT_SCHEMA}.join1_{id}_m; \
             analyze {DEFAULT_SCHEMA}.join1_{id}_j"
        ))
        .await
        .expect("analyze");
}

/// The estimated rows of `R`, `M` and `J`, in that order.
fn rows(status: &JoinStatus) -> Vec<(&'static str, i64)> {
    status
        .hidden_tables
        .iter()
        .map(|t| (t.table, t.estimated_rows))
        .collect()
}

#[tokio::test]
async fn status_reports_the_sizes_of_r_m_and_j() {
    let _serial = SERIAL.lock().await;
    let mut d = start(true).await;
    analyze(&d).await;
    let status = join_status(&d).await;
    let names: Vec<_> = status.hidden_tables.iter().map(|t| t.table).collect();
    assert_eq!(names, ["R", "M", "J"]);
    // 20 `a` rows, 6 `b` rows, and a `J` row for each of the 7 join keys that
    // either holds: 0 to 60.
    assert_eq!(rows(&status), [("R", 20), ("M", 6), ("J", 7)]);
    assert!(status.hidden_tables.iter().all(|t| t.bytes > 0));
    let id: i64 = d
        .ctl
        .query_one(
            "select id from transform_definitions where target_table = 'public.jl'",
            &[],
        )
        .await
        .expect("id")
        .get(0);
    let names: Vec<_> = status
        .hidden_tables
        .iter()
        .map(|t| t.name.clone())
        .collect();
    assert_eq!(
        names,
        [
            format!("join1_{id}_r"),
            format!("join1_{id}_m"),
            format!("join1_{id}_j")
        ]
    );
    assert_eq!(status.partials_warning, None);
    assert_eq!(status.fan_out_warning, None);
    assert_eq!(status.building, None);

    // `J` keeps a row for a key neither table holds any more, and gains one
    // for a new key; `M` keeps the tombstone of its deleted row.
    step(
        &mut d,
        "delete from public.a where k = 60; delete from public.b where k = 60",
    )
    .await;
    step(&mut d, "insert into public.a values ('a30', 70, 30, 's30')").await;
    analyze(&d).await;
    let status = join_status(&d).await;
    assert_eq!(rows(&status), [("R", 21), ("M", 6), ("J", 8)]);
    assert_eq!(status.building, None);
}

/// The warning's key, rows and threshold.
async fn warned(d: &Driver) -> Option<(String, i64, i64)> {
    join_status(d)
        .await
        .fan_out_warning
        .map(|w| (w.key, w.rows, w.threshold))
}

#[tokio::test]
async fn a_default_threshold_warns_of_nothing_in_a_small_table() {
    let _serial = SERIAL.lock().await;
    let d = start(true).await;
    let status = trellis(&d)
        .await
        .status("jl")
        .await
        .expect("status")
        .expect("exists");
    assert_eq!(status.join.expect("join").fan_out_warning, None);
}

#[tokio::test]
async fn status_warns_of_a_join_key_with_more_a_rows_than_the_threshold() {
    let _serial = SERIAL.lock().await;
    let mut d = start(true).await;
    // The largest group holds 3 rows (keys 20 and 40).
    set_one_to_one_join_fan_out_warning_rows(3);
    assert_eq!(
        join_status(&d).await.fan_out_warning,
        None,
        "3 is not past 3"
    );
    set_one_to_one_join_fan_out_warning_rows(2);
    let warning = join_status(&d)
        .await
        .fan_out_warning
        .expect("keys 20 and 40 hold 3 rows");
    assert_eq!((warning.rows, warning.threshold), (3, 2));
    assert!(warning.key == "20" || warning.key == "40", "{warning:?}");

    // Four more rows on key 30 make it the largest: 6.
    step(
        &mut d,
        "insert into public.a select 'x' || i, 30, 100 + i, 'x' from generate_series(1, 4) i",
    )
    .await;
    assert_eq!(warned(&d).await, Some(("30".to_string(), 6, 2)));
    set_one_to_one_join_fan_out_warning_rows(5);
    assert_eq!(warned(&d).await, Some(("30".to_string(), 6, 5)));
    set_one_to_one_join_fan_out_warning_rows(6);
    assert_eq!(
        join_status(&d).await.fan_out_warning,
        None,
        "6 is not past 6"
    );

    // Rows deleted are tombstones in `R`, and a tombstone is not a row on the
    // key; a row with a `NULL` key is on none.
    set_one_to_one_join_fan_out_warning_rows(3);
    step(&mut d, "delete from public.a where id like 'x%'").await;
    assert_eq!(join_status(&d).await.fan_out_warning, None);
    step(
        &mut d,
        "insert into public.a select 'n' || i, null, i, 'n' from generate_series(1, 9) i",
    )
    .await;
    assert_eq!(join_status(&d).await.fan_out_warning, None);
    set_one_to_one_join_fan_out_warning_rows(0);
}

/// The join key of a composite `ON` is named by all its columns.
#[tokio::test]
async fn a_composite_join_key_is_named_by_all_its_columns() {
    let _serial = SERIAL.lock().await;
    set_enabled(true);
    let d = Driver::start(
        "create table public.a (id integer primary key, k integer, m text); \
         create table public.b (id integer primary key, k integer, m text, w integer); \
         create unique index on public.b (k, m); \
         insert into public.a values (1, 1, 'x'), (2, 1, 'x'), (3, 1, 'x'), (4, 1, 'y'); \
         insert into public.b values (1, 1, 'x', 10)",
        &[
            ("k", ValueType::Integer(IntWidth::Int4)),
            ("m", ValueType::Text),
        ],
        &[
            "TRANSFORM jl FROM public.a LEFT JOIN public.b ON a.k = b.k AND a.m = b.m \
           SELECT b.w AS w",
        ],
        &["public.a", "public.b"],
    )
    .await;
    set_one_to_one_join_fan_out_warning_rows(2);
    let warning = join_status(&d)
        .await
        .fan_out_warning
        .expect("3 rows on (1, x)");
    assert_eq!(
        (warning.key.as_str(), warning.rows, warning.threshold),
        ("1,x", 3, 2)
    );
    set_one_to_one_join_fan_out_warning_rows(0);
}

// ----------------------------------------------------------- the build's step

fn options(chunk_rows: i64) -> WorkerOptions {
    WorkerOptions {
        chunk_rows,
        drain_batch_cap: 10_000,
        heartbeat_interval: std::time::Duration::from_secs(1),
        reclaim_ttl: std::time::Duration::from_secs(60),
    }
}

async fn build_step(d: &Driver, chunk_rows: i64) -> Step {
    build::work_once(d.pool(), "builder", &options(chunk_rows))
        .await
        .expect("a build step")
}

async fn building(d: &Driver) -> Option<OneToOneBuildStep> {
    match join_status(d).await.building {
        None => None,
        Some(JoinBuildStep::OneToOne(step)) => Some(step),
        Some(other) => panic!("a 1-1 JOIN reported {other:?}"),
    }
}

/// A fresh build: the plan job's `M` pass, then its chunks one by one, then
/// nothing once it is `live`.
#[tokio::test]
async fn status_names_each_step_of_a_fresh_build() {
    let _serial = SERIAL.lock().await;
    let d = start(false).await;
    assert_eq!(building(&d).await, Some(OneToOneBuildStep::JoinedRows));
    // The plan job runs the `M` pass, walks `a` in ranges of 3 and enqueues
    // 7 chunks.
    assert_eq!(build_step(&d, 3).await, Step::Planned);
    assert_eq!(
        building(&d).await,
        Some(OneToOneBuildStep::Chunks { remaining: 7 })
    );
    for remaining in (1..7).rev() {
        assert_eq!(build_step(&d, 3).await, Step::Chunk);
        assert_eq!(
            building(&d).await,
            Some(OneToOneBuildStep::Chunks { remaining }),
            "after a chunk"
        );
    }
    assert_eq!(build_step(&d, 3).await, Step::Chunk);
    assert_eq!(building(&d).await, None, "the build is done");
}

/// The stages of the plan job's `M` pass, by the cursor it stores.
#[tokio::test]
async fn status_names_the_stage_of_a_plan_job_by_its_cursor() {
    let _serial = SERIAL.lock().await;
    let d = start(false).await;
    for (cursor, step) in [
        (None, OneToOneBuildStep::JoinedRows),
        (Some("\u{1}b:3"), OneToOneBuildStep::JoinedRows),
        (Some("\u{1}n"), OneToOneBuildStep::JoinedNullKeys),
        (Some("\u{1}n:7"), OneToOneBuildStep::JoinedNullKeys),
        (Some("\u{1}g:4"), OneToOneBuildStep::JoinedGone),
        (Some("\u{1}a"), OneToOneBuildStep::PlanningChunks),
        (Some("\u{1}a:a07"), OneToOneBuildStep::PlanningChunks),
    ] {
        d.ctl
            .execute(
                "update backfill_chunks set lo = $1 where kind = 'plan'",
                &[&cursor],
            )
            .await
            .expect("set the cursor");
        assert_eq!(building(&d).await, Some(step), "{cursor:?}");
    }
}

/// A rebuild in place of a live join, with a target that has rows, ends with
/// a sweep: its stale `R` entries, then its orphans. A definition that is
/// paused reports no step.
#[tokio::test]
async fn status_names_the_steps_of_a_rebuild_and_its_sweep() {
    let _serial = SERIAL.lock().await;
    let mut d = start(true).await;
    d.retire().await;
    trellis(&d)
        .await
        .request_backfill("public.a")
        .await
        .expect("request a rebuild");
    assert_eq!(building(&d).await, Some(OneToOneBuildStep::JoinedRows));
    assert_eq!(build_step(&d, 3).await, Step::Planned);
    assert_eq!(
        building(&d).await,
        Some(OneToOneBuildStep::Chunks { remaining: 7 })
    );
    for _ in 0..7 {
        assert_eq!(build_step(&d, 3).await, Step::Chunk);
    }
    assert_eq!(building(&d).await, Some(OneToOneBuildStep::SweepStale));
    // The sweep's stages are by its stored cursor.
    for (cursor, step) in [
        (Some("\u{1}r:a05"), OneToOneBuildStep::SweepStale),
        (Some("\u{1}o:a05"), OneToOneBuildStep::SweepOrphans),
        (Some("\u{1}n"), OneToOneBuildStep::SweepOrphans),
        (None, OneToOneBuildStep::SweepStale),
    ] {
        d.ctl
            .execute(
                "update backfill_chunks set lo = $1 where kind = 'sweep' and not done",
                &[&cursor],
            )
            .await
            .expect("set the cursor");
        assert_eq!(building(&d).await, Some(step), "{cursor:?}");
    }
    // A paused definition's job is not handed out, so it names no step.
    d.ctl
        .batch_execute(
            "update transform_definitions set status = 'paused' \
             where target_table = 'public.jl'",
        )
        .await
        .expect("pause");
    assert_eq!(building(&d).await, None);
    d.ctl
        .batch_execute(
            "update transform_definitions set status = 'backfilling' \
             where target_table = 'public.jl'",
        )
        .await
        .expect("unpause");
    // The sweep runs to its end.
    for _ in 0..20 {
        if building(&d).await.is_none() {
            break;
        }
        build_step(&d, 3).await;
    }
    assert_eq!(building(&d).await, None);
}

/// An `R` larger than the sample reads a share of its pages: the group that
/// holds two fifths of it is among the keys met most often, and its count over
/// the join-key index is exact. (The entries are written to `R` directly, so
/// the test doesn't build 300k rows.)
#[tokio::test]
async fn a_sampled_r_still_finds_the_large_group_and_counts_it_exactly() {
    let _serial = SERIAL.lock().await;
    let d = start(true).await;
    let id: i64 = d
        .ctl
        .query_one(
            "select id from transform_definitions where target_table = 'public.jl'",
            &[],
        )
        .await
        .expect("id")
        .get(0);
    let r = format!("{DEFAULT_SCHEMA}.join1_{id}_r");
    d.ctl
        .batch_execute(&format!(
            "insert into {r} (__from_key, k, r) \
                 select 'big' || lpad(i::text, 7, '0'), 55, i from generate_series(1, 200000) i; \
             insert into {r} (__from_key, k, r) \
                 select 'one' || lpad(i::text, 7, '0'), 1000 + i, i \
                 from generate_series(1, 300000) i"
        ))
        .await
        .expect("fill R");
    let pages: i64 = d
        .ctl
        .query_one(
            &format!("select pg_relation_size('{r}') / current_setting('block_size')::bigint"),
            &[],
        )
        .await
        .expect("pages")
        .get(0);
    assert!(
        pages > 2000,
        "R has {pages} pages: the sample is a share of it"
    );
    set_one_to_one_join_fan_out_warning_rows(50_000);
    assert_eq!(warned(&d).await, Some(("55".to_string(), 200_000, 50_000)));
    // A threshold above the group's size warns of nothing.
    set_one_to_one_join_fan_out_warning_rows(200_000);
    assert_eq!(warned(&d).await, None);
    set_one_to_one_join_fan_out_warning_rows(0);
}
