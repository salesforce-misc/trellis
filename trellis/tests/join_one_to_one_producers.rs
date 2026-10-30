//! The producers of Re-derives of a 1-1 `JOIN` (ADR-0021, #1401): what stages
//! or starts one apart from the drain's own pages. A build chunk, a rebuild
//! and `RESUME` are covered by `join_one_to_one_build.rs`, a Re-derive page of
//! either side by `join_one_to_one_interleavings.rs`; this file holds what
//! those leave out.
//!
//! - **`NULL` primary-key parts.** A side that is an aggregate's target is
//!   keyed by its nullable `GROUP BY` column (`unique nulls not distinct`).
//!   A build chunk's range is a row comparison, which is `NULL` wherever a
//!   `NULL` part decides it, so no chunk reaches such an `a` row: the plan
//!   job stages a Re-derive of each at its end, and the page writes its
//!   target row under a `NULL` key. A `b` change fans out to such rows when
//!   the `ON` leaves out their `NULL` part. A `b` row keyed that way is read
//!   by the `M` pass's own batch.
//! - **An upstream's rebuild.** A `RESUME` of the aggregate the join reads
//!   rewrites its target through the target-mutation seam, and the join
//!   follows its rows.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::integer::IntWidth;

/// The dev knob and the fan-out cap are process-wide, so these tests run one
/// at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

fn int8() -> ValueType {
    ValueType::Integer(IntWidth::Int8)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Left,
    Inner,
}

const KINDS: [Kind; 2] = [Kind::Left, Kind::Inner];

impl Kind {
    fn word(self) -> &'static str {
        match self {
            Kind::Left => "LEFT",
            Kind::Inner => "INNER",
        }
    }

    fn sql(self) -> &'static str {
        match self {
            Kind::Left => "left",
            Kind::Inner => "inner",
        }
    }
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it, and everything it leads to.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
    d.settle().await;
}

/// Installs the 1-1 `JOIN` `definition` over live upstreams and captures
/// `tables`.
async fn define_join(d: &mut Driver, definition: &str, tables: &[&str]) {
    trellis::defs::install_definition(
        d.pool(),
        definition,
        &HashMap::from([("total".to_string(), int8())]),
        "public",
    )
    .await
    .expect("define the 1-1 JOIN");
    for table in tables {
        let catalog = trellis::capture::columns::load_catalog(&d.ctl)
            .await
            .expect("load the catalog");
        let spec = trellis::capture::columns::capture_spec(&d.ctl, &catalog, table)
            .await
            .expect("capture spec");
        match trellis::capture::install::reconcile(&mut d.ctl, DEFAULT_SCHEMA, &spec, None)
            .await
            .expect("capture the table")
        {
            trellis::capture::install::Progress::Done(_) => {}
            trellis::capture::install::Progress::Waiting(wait) => {
                panic!("capture waited: {wait}")
            }
        }
    }
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
}

async fn definition_id(d: &Driver, target: &str) -> i64 {
    d.ctl
        .query_one(
            "select id from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("the definition")
        .get(0)
}

async fn status(d: &Driver, target: &str) -> String {
    d.ctl
        .query_one(
            "select status::text from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("the definition")
        .get(0)
}

// ------------------------------------- an `a` side keyed by a nullable column

/// An aggregate's target as `a`: `agg` counts `src` rows per group, some
/// keyed `NULL` in one or more parts, and `b` is keyed on the group's
/// columns.
struct Case {
    ddl: &'static str,
    /// `src`'s columns beside its key, for the define.
    columns: Vec<(&'static str, ValueType)>,
    aggregate: &'static str,
    /// The `ON` of the join.
    on: &'static str,
    /// `agg`'s key columns as text, and the order that sorts by them.
    key: &'static str,
    order: &'static str,
    /// Statements that change the `NULL`-keyed groups: grow one, delete
    /// them, bring one back.
    changes: [&'static str; 3],
}

/// One key column.
fn one_column() -> Case {
    Case {
        ddl: "create table public.src (id integer primary key, g integer, v integer); \
              create table public.b (id integer primary key, k integer, w integer); \
              create unique index on public.b (k); \
              insert into public.src values \
                  (1, 1, 10), (2, null, 20), (3, 2, 5), (4, null, 7), (5, 3, 1); \
              insert into public.b values (1, 1, 100), (2, 2, 200), (3, null, 300)",
        columns: vec![("g", int4()), ("v", int4())],
        aggregate: "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
                    SELECT count(*) AS n",
        on: "agg.g = b.k",
        key: "agg.g::text",
        order: "1",
        changes: [
            "insert into public.src values (6, null, 1)",
            "delete from public.src where g is null",
            "insert into public.src values (7, null, 1)",
        ],
    }
}

/// Two key columns: a key with one `NULL` part, or the other, or both.
fn two_columns() -> Case {
    Case {
        ddl: "create table public.src (id integer primary key, g integer, h integer, \
                  v integer); \
              create table public.b (id integer primary key, k integer, m integer, \
                  w integer); \
              create unique index on public.b (k, m); \
              insert into public.src values \
                  (1, 1, 1, 10), (2, 1, null, 20), (3, null, 1, 5), (4, null, null, 7), \
                  (5, 2, 2, 1), (6, 1, null, 3); \
              insert into public.b values (1, 1, 1, 100), (2, 2, 2, 200), (3, 1, null, 300)",
        columns: vec![("g", int4()), ("h", int4()), ("v", int4())],
        aggregate: "TRANSFORM agg FROM public.src GROUP BY g, h WITHOUT SOURCE INDEX \
                    SELECT count(*) AS n",
        on: "agg.g = b.k AND agg.h = b.m",
        key: "agg.g::text, agg.h::text",
        order: "1, 2",
        changes: [
            "insert into public.src values (7, null, 1, 1), (8, 1, null, 1), (9, null, null, 1)",
            "delete from public.src where g is null or h is null",
            "insert into public.src values (10, null, 1, 1), (11, 1, null, 1), \
                                           (12, null, null, 1)",
        ],
    }
}

async fn start_nullable_a(case: &Case, kind: Kind) -> Driver {
    set_enabled(true);
    let mut d = Driver::start(case.ddl, &case.columns, &[case.aggregate], &["public.src"]).await;
    define_join(
        &mut d,
        &format!(
            "TRANSFORM jn FROM public.agg {} JOIN public.b ON {} \
             SELECT agg.n + COALESCE(b.w, 0) AS total",
            kind.word(),
            case.on
        ),
        &["public.b"],
    )
    .await;
    d
}

/// The target equals the live join, by `self_check`'s comparison and by the
/// rows, and `R` holds a live entry for every `agg` row (the `NULL`-keyed
/// groups among them).
async fn assert_a_oracle(d: &Driver, case: &Case, kind: Kind, step: &str) {
    let target_key = case.key.replace("agg.", "");
    let actual = d
        .rows(&format!(
            "select {target_key}, total::text from public.jn order by {}",
            case.order
        ))
        .await;
    let expected = d
        .rows(&format!(
            "select {}, (agg.n + coalesce(b.w, 0))::text from public.agg agg \
             {} join public.b b on {} order by {}",
            case.key,
            kind.sql(),
            case.on,
            case.order
        ))
        .await;
    assert_eq!(
        actual, expected,
        "{kind:?} target (left) differs from the live join (right) after {step}"
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), "public.jn")
        .await
        .expect("self_check");
    assert!(diverging.is_empty(), "{kind:?} after {step}: {diverging:?}");
    let id = definition_id(d, "public.jn").await;
    let held: i64 = d
        .ctl
        .query_one(
            &format!("select count(*) from {DEFAULT_SCHEMA}.join1_{id}_r where not __tombstone"),
            &[],
        )
        .await
        .expect("count R")
        .get(0);
    let live: i64 = d
        .ctl
        .query_one("select count(*) from public.agg", &[])
        .await
        .expect("count agg")
        .get(0);
    assert_eq!(held, live, "{kind:?} R after {step}");
    assert_eq!(status(d, "public.jn").await, "live", "after {step}");
}

/// A 1-1 `JOIN` over an aggregate's target as `a`: the build reaches the rows
/// keyed `NULL`, which no chunk's range does, and the pages that change them
/// after write and delete their target rows.
async fn nulls_in_a_are_built_and_kept(case: Case) {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start_nullable_a(&case, kind).await;
        assert_a_oracle(&d, &case, kind, "the build").await;
        if kind == Kind::Left {
            let nulls: i64 = d
                .ctl
                .query_one(
                    "select count(*) from public.jn where g is null or total is null",
                    &[],
                )
                .await
                .expect("count")
                .get(0);
            assert!(nulls > 0, "a row keyed NULL is in the target");
        }
        let [grow, delete, back] = case.changes;
        // A group's count changes, through the seam's images.
        step(&mut d, grow).await;
        assert_a_oracle(&d, &case, kind, "a NULL-keyed group grows").await;
        // The groups go, and come back.
        step(&mut d, delete).await;
        assert_a_oracle(&d, &case, kind, "the NULL-keyed groups deleted").await;
        step(&mut d, back).await;
        assert_a_oracle(&d, &case, kind, "the NULL-keyed groups back").await;
    }
}

#[tokio::test]
async fn a_row_of_a_keyed_by_null_is_built_and_kept() {
    nulls_in_a_are_built_and_kept(one_column()).await;
}

#[tokio::test]
async fn a_row_of_a_keyed_by_null_in_some_parts_is_built_and_kept() {
    nulls_in_a_are_built_and_kept(two_columns()).await;
}

async fn trellis(d: &Driver) -> trellis::Trellis {
    trellis::Trellis::connect(
        trellis::config::Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

/// Two key columns, joined on the first alone: a group keyed `NULL` in its
/// second part joins a `b` row, so a `b` change fans out to it beside the
/// groups with no `NULL` part, in one target write.
fn joined_on_first_column() -> Case {
    Case {
        ddl: "create table public.src (id integer primary key, g integer, h integer, \
                  v integer); \
              create table public.b (id integer primary key, k integer, w integer); \
              create unique index on public.b (k); \
              insert into public.src values \
                  (1, 1, 1, 10), (2, 1, null, 20), (3, null, 1, 5), (4, null, null, 7), \
                  (5, 2, null, 1); \
              insert into public.b values (1, 1, 100), (2, 2, 200)",
        columns: vec![("g", int4()), ("h", int4()), ("v", int4())],
        aggregate: "TRANSFORM agg FROM public.src GROUP BY g, h WITHOUT SOURCE INDEX \
                    SELECT count(*) AS n",
        on: "agg.g = b.k",
        key: "agg.g::text, agg.h::text",
        order: "1, 2",
        changes: [
            "insert into public.src values (6, 1, null, 1), (7, null, null, 1)",
            "delete from public.src where g is null or h is null",
            "insert into public.src values (8, 1, null, 1), (9, 2, null, 1)",
        ],
    }
}

/// A `b` change fans out to the `a` rows on its key, the ones keyed `NULL` in
/// a part the `ON` doesn't name among them: an update rewrites their target
/// rows, a delete removes them (`INNER`) or empties their `b` columns
/// (`LEFT`), an insert brings them back, and a key's move does both.
#[tokio::test]
async fn a_b_change_fans_out_to_the_rows_of_a_keyed_by_null() {
    let _serial = SERIAL.lock().await;
    let case = joined_on_first_column();
    for kind in KINDS {
        let mut d = start_nullable_a(&case, kind).await;
        assert_a_oracle(&d, &case, kind, "the build").await;
        for (sql, what) in [
            ("update public.b set w = 150 where k = 1", "an update of b"),
            ("delete from public.b where k = 1", "a delete of b"),
            ("insert into public.b values (3, 1, 175)", "an insert of b"),
            ("update public.b set k = 3 where id = 2", "a key's move"),
            ("update public.b set k = 2 where id = 2", "a move back"),
        ] {
            step(&mut d, sql).await;
            assert_a_oracle(&d, &case, kind, what).await;
        }
        nulls_changes(&mut d, &case, kind).await;
    }
}

/// [`Case::changes`] in turn, each checked against the live join.
async fn nulls_changes(d: &mut Driver, case: &Case, kind: Kind) {
    let [grow, delete, back] = case.changes;
    step(d, grow).await;
    assert_a_oracle(d, case, kind, "a NULL-keyed group grows").await;
    step(d, delete).await;
    assert_a_oracle(d, case, kind, "the NULL-keyed groups deleted").await;
    step(d, back).await;
    assert_a_oracle(d, case, kind, "the NULL-keyed groups back").await;
}

/// A `RESUME` of the aggregate the join reads as `a`: the aggregate's
/// rebuild rewrites its target, the groups keyed `NULL` among them, and the
/// join follows. Every write of the rebuild reaches the join through the
/// target-mutation seam as the key's new row (or a re-read of it), and a 1-1
/// `JOIN` replaces its `R` entry with it rather than applying a delta from
/// the prior image, so the join is exact once the seam's rows drain whether
/// or not the reader rebuild the aggregate's resume starts
/// (`intake::catch_up::park_target_catchup_if_read`) has run.
#[tokio::test]
async fn an_upstream_resume_is_followed_by_the_join() {
    let _serial = SERIAL.lock().await;
    for case in [one_column(), two_columns(), joined_on_first_column()] {
        for kind in KINDS {
            let mut d = start_nullable_a(&case, kind).await;
            let trellis = trellis(&d).await;
            trellis.apply("PAUSE TRANSFORM agg").await.expect("pause");
            let [grow, delete, back] = case.changes;
            step(&mut d, grow).await;
            step(&mut d, delete).await;
            trellis.apply("RESUME TRANSFORM agg").await.expect("resume");
            trellis::intake::harness::start_registered_builds(d.pool())
                .await
                .expect("start the resumed build");
            trellis::staging::build::settle_builds(d.pool()).await;
            d.settle().await;
            assert_eq!(status(&d, "public.agg").await, "live");
            assert_a_oracle(&d, &case, kind, "the aggregate's resume").await;
            step(&mut d, back).await;
            assert_a_oracle(&d, &case, kind, "a change after the resume").await;
        }
    }
}

/// A rebuild in place of a live join (`request_backfill` of `b`, after a
/// change capture never saw) re-derives the rows of `a` keyed `NULL` with the
/// rest.
#[tokio::test]
async fn a_rebuild_in_place_keeps_the_rows_of_a_keyed_by_null() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let case = one_column();
        let mut d = start_nullable_a(&case, kind).await;
        d.ctl
            .batch_execute(
                "do $$ declare t record; begin \
                     for t in select tgname from pg_trigger \
                              where tgrelid = 'public.b'::regclass and not tgisinternal loop \
                         execute format('alter table public.b disable trigger %I', t.tgname); \
                     end loop; \
                 end $$; \
                 update public.b set w = 150 where id = 1; \
                 do $$ declare t record; begin \
                     for t in select tgname from pg_trigger \
                              where tgrelid = 'public.b'::regclass and not tgisinternal loop \
                         execute format('alter table public.b enable always trigger %I', \
                                        t.tgname); \
                     end loop; \
                 end $$;",
            )
            .await
            .expect("write past capture");
        trellis(&d)
            .await
            .request_backfill("public.b")
            .await
            .expect("request a rebuild");
        trellis::staging::build::settle_builds(d.pool()).await;
        d.settle().await;
        assert_a_oracle(&d, &case, kind, "the rebuild").await;
    }
}

/// A `RESUME` keeps the target and recreates `R` empty: the target row of a
/// group keyed `NULL` that went while the join was paused is removed by the
/// sweep's orphan pass, and a group that came is built.
#[tokio::test]
async fn a_resume_after_the_null_keyed_groups_change_while_paused() {
    let _serial = SERIAL.lock().await;
    for case in [one_column(), two_columns()] {
        for kind in KINDS {
            let mut d = start_nullable_a(&case, kind).await;
            let trellis = trellis(&d).await;
            trellis.apply("PAUSE TRANSFORM jn").await.expect("pause");
            let [grow, delete, back] = case.changes;
            step(&mut d, grow).await;
            step(&mut d, delete).await;
            trellis.apply("RESUME TRANSFORM jn").await.expect("resume");
            trellis::intake::harness::start_registered_builds(d.pool())
                .await
                .expect("start the resumed build");
            trellis::staging::build::settle_builds(d.pool()).await;
            d.settle().await;
            assert_a_oracle(&d, &case, kind, "a resume after the groups went").await;
            trellis.apply("PAUSE TRANSFORM jn").await.expect("pause");
            step(&mut d, back).await;
            trellis.apply("RESUME TRANSFORM jn").await.expect("resume");
            trellis::intake::harness::start_registered_builds(d.pool())
                .await
                .expect("start the resumed build");
            trellis::staging::build::settle_builds(d.pool()).await;
            d.settle().await;
            assert_a_oracle(&d, &case, kind, "a resume after the groups came back").await;
        }
    }
}

// ------------------------------------- a `b` side keyed by a nullable column

const B_DDL: &str = "create table public.src (id integer primary key, g integer, v integer); \
                     create table public.a (id integer primary key, k integer, x integer); \
                     create index on public.a (k); \
                     insert into public.src values \
                         (1, 1, 10), (2, null, 20), (3, 2, 5), (4, null, 7); \
                     insert into public.a values (1, 1, 1), (2, 2, 2), (3, null, 3), (4, 9, 4)";

/// The target equals the live join, and `M` holds an entry for every `agg`
/// row, the group keyed `NULL` among them.
async fn assert_b_oracle(d: &Driver, kind: Kind, step: &str) {
    let actual = d
        .rows("select id::text, total::text from public.jn order by 1")
        .await;
    let expected = d
        .rows(&format!(
            "select a.id::text, (a.x + coalesce(agg.n, 0))::text from public.a a \
             {} join public.agg agg on a.k = agg.g order by 1",
            kind.sql()
        ))
        .await;
    assert_eq!(actual, expected, "{kind:?} target after {step}");
    let id = definition_id(d, "public.jn").await;
    let held: i64 = d
        .ctl
        .query_one(
            &format!("select count(*) from {DEFAULT_SCHEMA}.join1_{id}_m where not __tombstone"),
            &[],
        )
        .await
        .expect("count M")
        .get(0);
    let live: i64 = d
        .ctl
        .query_one("select count(*) from public.agg", &[])
        .await
        .expect("count agg")
        .get(0);
    assert_eq!(held, live, "{kind:?} M after {step}");
    assert_eq!(live, 3, "the aggregate has a group keyed NULL");
}

/// A 1-1 `JOIN` over an aggregate's target as `b`: the `M` pass reads the
/// group keyed `NULL`, which no batch of `b`'s key order does, so `M` holds an
/// entry for every `agg` row. No `a` row joins it (a `NULL` key never
/// matches), and the pages that change the group leave the target as it was.
#[tokio::test]
async fn a_row_of_b_keyed_by_null_is_read_by_the_m_pass() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        set_enabled(true);
        let mut d = Driver::start(
            B_DDL,
            &one_column().columns,
            &["TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT count(*) AS n"],
            &["public.src"],
        )
        .await;
        define_join(
            &mut d,
            &format!(
                "TRANSFORM jn FROM public.a {} JOIN public.agg ON a.k = agg.g \
                 SELECT a.x + COALESCE(agg.n, 0) AS total",
                kind.word()
            ),
            &["public.a"],
        )
        .await;
        assert_b_oracle(&d, kind, "the build").await;
        step(
            &mut d,
            "insert into public.src values (5, null, 1), (6, 1, 1)",
        )
        .await;
        assert_b_oracle(&d, kind, "the NULL group grows").await;
    }
}
