//! A 1-1 target over a source whose identity key is nullable (#1141): an
//! aggregate target's `GROUP BY` columns, keyed `UNIQUE NULLS NOT DISTINCT`.
//!
//! The 1-1 target takes the same identity: a `UNIQUE NULLS NOT DISTINCT`
//! constraint over the source key's columns, each as nullable as it is on
//! the source, so a group whose key has a `NULL` part has its row in the
//! reader as it has in the aggregate. Each test drives the pipeline by hand
//! through `support/drain_driver.rs` (no sleeping or polling for
//! convergence, #297), and compares every target with a `GROUP BY` over the
//! source: the oracle. The aggregate and `JOIN` sources are in
//! `group_basis_chained.rs`.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
use std::time::Duration;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page,
};

const SRC: &str = "public.src";
const SRC_DDL: &str = "create table public.src (id integer primary key, g integer, h integer, \
                       v integer)";

/// A composite `GROUP BY`, either part of which can be `NULL`.
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g, h WITHOUT SOURCE INDEX SELECT COUNT(*) AS n, SUM(v) AS t";
/// The 1-1 reader of `agg`.
const A2: &str = "TRANSFORM a2 FROM public.agg SELECT n AS n, t + 1 AS t1";

const A2_ACTUAL: &str = "select g, h, n, t1 from public.a2 order by g, h";
const A2_ORACLE: &str =
    "select g, h, count(*), sum(v) + 1 from public.src group by g, h order by g, h";
const AGG_ACTUAL: &str = "select g, h, n, t from public.agg order by g, h";
const AGG_ORACLE: &str =
    "select g, h, count(*), sum(v) from public.src group by g, h order by g, h";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

fn int8() -> ValueType {
    ValueType::Integer(IntWidth::Int8)
}

fn src_columns() -> Vec<(&'static str, ValueType)> {
    vec![("id", int4()), ("g", int4()), ("h", int4()), ("v", int4())]
}

fn agg_columns() -> Vec<(&'static str, ValueType)> {
    vec![("g", int4()), ("h", int4()), ("n", int8()), ("t", int8())]
}

async fn install(d: &Driver, definition: &str, columns: &[(&str, ValueType)]) {
    let columns: HashMap<String, ValueType> = columns
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect();
    trellis::defs::install_definition(d.pool(), definition, &columns, "public")
        .await
        .unwrap_or_else(|e| panic!("install {definition}: {e}"));
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it, and everything it propagates, to quiescence.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    d.settle().await;
}

/// Asserts `actual` equals the oracle `expected`, both over the settled
/// pipeline.
async fn assert_equal(d: &mut Driver, actual: &str, expected: &str) {
    d.settle().await;
    assert_eq!(
        d.rows(actual).await,
        d.rows(expected).await,
        "{actual} (left) differs from the oracle (right)"
    );
}

/// Both targets equal the oracle.
async fn assert_both(d: &mut Driver) {
    assert_equal(d, AGG_ACTUAL, AGG_ORACLE).await;
    assert_equal(d, A2_ACTUAL, A2_ORACLE).await;
}

/// `public.src`, captured, with `agg` live over it, `seed` written and
/// drained, then `a2` defined over `agg` and built to `live`.
async fn start(seed: Option<&str>) -> Driver {
    let mut d = Driver::start(SRC_DDL, &src_columns(), &[AGG], &[SRC]).await;
    if let Some(seed) = seed {
        step(&mut d, seed).await;
    }
    install(&d, A2, &agg_columns()).await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_live(&d).await;
    d
}

async fn assert_live(d: &Driver) {
    assert_eq!(
        d.rows("select status from transform_definitions where status <> 'live'")
            .await,
        Vec::<String>::new(),
        "every definition is live"
    );
}

/// The issue's repro: a group with a `NULL` key part reaches the reader.
#[tokio::test]
async fn a_group_with_a_null_key_part_reaches_the_one_to_one_reader() {
    let mut d = start(None).await;
    step(
        &mut d,
        "insert into public.src values (1, 1, null, 1), (2, 1, 2, 1)",
    )
    .await;
    assert_both(&mut d).await;
    assert_eq!(
        d.rows("select g, h, n from public.a2 where h is null")
            .await,
        ["(1,,1)"],
        "the NULL-keyed group has its row in the reader"
    );
}

/// The reader's key is the source's identity: `UNIQUE NULLS NOT DISTINCT`
/// over the nullable columns, not a primary key.
#[tokio::test]
async fn the_reader_is_keyed_by_a_nulls_not_distinct_unique_constraint() {
    let d = start(None).await;
    assert_eq!(
        d.rows(
            "select i.indisprimary, i.indisunique, i.indnullsnotdistinct \
             from pg_index i where i.indrelid = 'public.a2'::regclass"
        )
        .await,
        ["(f,t,t)"]
    );
    assert_eq!(
        d.rows(
            "select attname, attnotnull from pg_attribute \
             where attrelid = 'public.a2'::regclass and attname in ('g', 'h') order by attname"
        )
        .await,
        ["(g,f)", "(h,f)"]
    );
    // A 1-1 target over a primary key keeps its primary key.
    install(
        &d,
        "TRANSFORM one FROM public.src SELECT v AS v",
        &src_columns(),
    )
    .await;
    assert_eq!(
        d.rows("select i.indisprimary from pg_index i where i.indrelid = 'public.one'::regclass")
            .await,
        ["(t)"]
    );
}

/// A row moves into, within and out of a `NULL`-keyed group, by insert,
/// update of a group column to and from `NULL`, and delete; the group with
/// both parts `NULL` included.
#[tokio::test]
async fn rows_moving_into_within_and_out_of_a_null_keyed_group() {
    let mut d = start(Some(
        "insert into public.src values (1, 1, 1, 1), (2, 1, 2, 2), (3, 2, 1, 3)",
    ))
    .await;
    assert_both(&mut d).await;
    // Into: an insert creates the group, a second grows it.
    step(&mut d, "insert into public.src values (4, 1, null, 4)").await;
    assert_both(&mut d).await;
    step(&mut d, "insert into public.src values (5, 1, null, 5)").await;
    assert_both(&mut d).await;
    // Into by update of a group column to NULL, and both parts NULL.
    step(&mut d, "update public.src set h = null where id = 2").await;
    assert_both(&mut d).await;
    step(
        &mut d,
        "update public.src set g = null, h = null where id = 3",
    )
    .await;
    assert_both(&mut d).await;
    // Within: a member's value changes, and the group's key stays.
    step(&mut d, "update public.src set v = 40 where id = 4").await;
    assert_both(&mut d).await;
    // Out of it by update from NULL, and between two NULL-keyed groups.
    step(&mut d, "update public.src set h = 7 where id = 5").await;
    assert_both(&mut d).await;
    step(&mut d, "update public.src set g = null where id = 4").await;
    assert_both(&mut d).await;
    // Out by delete: each NULL-keyed group goes extinct.
    step(&mut d, "delete from public.src where id = 2").await;
    assert_both(&mut d).await;
    step(&mut d, "delete from public.src where id in (3, 4)").await;
    assert_both(&mut d).await;
    assert_eq!(
        d.rows("select g, h from public.a2 where g is null or h is null")
            .await,
        Vec::<String>::new(),
        "every NULL-keyed group is gone from the reader"
    );
}

/// A reader defined over an aggregate that already holds only a
/// `NULL`-keyed group: the build writes its row.
#[tokio::test]
async fn a_build_over_a_one_row_source_with_a_null_key_writes_it() {
    let mut d = start(Some("insert into public.src values (1, null, null, 1)")).await;
    assert_both(&mut d).await;
    assert_eq!(d.rows("select g, h, n from public.a2").await, ["(,,1)"]);
}

/// A chunked build of the reader, over an aggregate with groups of every
/// `NULL` pattern among others: a few rows per chunk, so the range walk takes
/// many chunks and no range reaches a `NULL`-keyed group.
#[tokio::test]
async fn a_chunked_build_writes_every_null_keyed_group() {
    let mut d = Driver::start(SRC_DDL, &src_columns(), &[AGG], &[SRC]).await;
    step(
        &mut d,
        "insert into public.src \
         select i, case when i % 5 = 0 then null else i % 7 end, \
                case when i % 3 = 0 then null else i % 4 end, i \
         from generate_series(1, 60) i",
    )
    .await;
    install(&d, A2, &agg_columns()).await;
    trellis::intake::markers::discharge_registrations(d.pool())
        .await
        .expect("start the reader's build");
    let options = WorkerOptions {
        chunk_rows: 3,
        drain_batch_cap: 100_000,
        heartbeat_interval: Duration::from_secs(1),
        reclaim_ttl: Duration::from_secs(30),
    };
    let mut chunks = 0;
    for _ in 0..500 {
        d.settle().await;
        let step = build::work_once(d.pool(), "worker", &options)
            .await
            .expect("build step");
        if step == build::Step::Chunk {
            chunks += 1;
        }
        if !step.progressed() {
            break;
        }
    }
    d.settle().await;
    assert_live(&d).await;
    assert!(chunks > 3, "the build took many chunks: {chunks}");
    assert!(
        d.rows("select 1 from public.agg where g is null or h is null")
            .await
            .len()
            >= 3,
        "the source has NULL-keyed groups of each pattern"
    );
    assert_both(&mut d).await;
}

/// `self_check` of the reader compares its `NULL`-keyed rows: clean once
/// converged, and a planted divergence on a `NULL`-keyed row is reported.
#[tokio::test]
async fn self_check_compares_null_keyed_rows() {
    let mut d = start(Some(
        "insert into public.src values (1, 1, null, 1), (2, null, null, 2), (3, 2, 2, 3)",
    ))
    .await;
    assert_both(&mut d).await;
    let check = async |d: &Driver| -> Vec<Divergence> {
        let page = check_page(
            d.pool(),
            "a2",
            SelfCheckScope {
                after: None,
                limit: 100,
            },
            SelfCheckMode::Standard,
            Duration::from_secs(30),
        )
        .await
        .expect("a page of the check");
        assert_eq!(page.rows_compared, 3, "every row, NULL-keyed ones included");
        match page.outcome {
            SelfCheckOutcome::Converged => Vec::new(),
            SelfCheckOutcome::Diverged(divergences) => divergences,
            other => panic!("unexpected outcome {other:?}"),
        }
    };
    assert_eq!(check(&d).await, Vec::new(), "a converged reader is clean");

    d.ctl
        .batch_execute("update public.a2 set n = 99 where g = 1 and h is null")
        .await
        .expect("plant a divergence");
    let found = check(&d).await;
    assert!(
        matches!(found.as_slice(), [Divergence::Cell { column, persisted, recomputed, .. }]
            if column == "n" && persisted.as_deref() == Some("99")
                && recomputed.as_deref() == Some("1")),
        "the planted cell on the NULL-keyed row, alone: {found:?}"
    );
}

/// A chain of three: an aggregate, a 1-1 over it and a 1-1 over that. The
/// middle target, keyed `UNIQUE NULLS NOT DISTINCT`, resolves as the last
/// one's source key, and both readers hold the `NULL` group.
#[tokio::test]
async fn a_chain_of_aggregate_and_two_one_to_one_readers_keeps_the_null_group() {
    let mut d = Driver::start(
        SRC_DDL,
        &src_columns(),
        &["TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total"],
        &[SRC],
    )
    .await;
    install(
        &d,
        "TRANSFORM b FROM public.agg SELECT total + total AS dbl",
        &[("g", int4()), ("total", int8())],
    )
    .await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    install(
        &d,
        "TRANSFORM c FROM public.b SELECT dbl + 1 AS inc",
        &[("g", int4()), ("dbl", int8())],
    )
    .await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_live(&d).await;

    const C_ACTUAL: &str = "select g, inc from public.c order by g";
    const C_ORACLE: &str = "select g, sum(v) * 2 + 1 from public.src group by g order by g";
    for sql in [
        "insert into public.src values (1, 1, 0, 1), (2, null, 0, 2), (3, null, 0, 3)",
        "update public.src set g = null where id = 1",
        "update public.src set g = 4 where id = 2",
        "delete from public.src where g is null",
    ] {
        step(&mut d, sql).await;
        assert_equal(&mut d, C_ACTUAL, C_ORACLE).await;
    }
    step(&mut d, "insert into public.src values (5, null, 0, 5)").await;
    assert_equal(&mut d, C_ACTUAL, C_ORACLE).await;
    assert_eq!(
        d.rows("select g, inc from public.c where g is null").await,
        ["(,11)"]
    );

    // The evaluator-driven oracle agrees, the NULL key included.
    let def =
        trellis::defs::parse("TRANSFORM c FROM public.b SELECT dbl + 1 AS inc").expect("parse");
    let columns: HashMap<String, ValueType> =
        [("g".to_string(), int4()), ("dbl".to_string(), int8())]
            .into_iter()
            .collect();
    let recomputed = trellis::defs::oracle::recompute(d.pool(), &def, "g", &columns)
        .await
        .expect("recompute");
    let null_key = recomputed
        .get("\u{1}")
        .expect("the oracle has the NULL key, under the key sentinel");
    assert_eq!(
        null_key
            .get("inc")
            .cloned()
            .flatten()
            .map(|v| v.to_string()),
        Some("11".to_string())
    );
    assert_eq!(recomputed.len(), 2);
}

/// A rebuild's sweep re-derives the reader's `NULL`-keyed entries too: it
/// restores a `NULL`-keyed row whose value went stale, and deletes one whose
/// source row went away, as it does any other key's.
#[tokio::test]
async fn a_rebuild_sweep_rederives_null_keyed_rows() {
    let mut d = start(Some(
        "insert into public.src values (1, 1, null, 1), (2, null, null, 2), (3, 2, 2, 3)",
    ))
    .await;
    assert_both(&mut d).await;
    // As a frozen definition misses them: a stale value on one NULL-keyed
    // row, and the source row of the other gone.
    d.ctl
        .batch_execute(
            "update public.a2 set n = 99 where g = 1 and h is null; \
             delete from public.agg where g is null and h is null",
        )
        .await
        .expect("plant what a frozen reader would miss");
    // A start after every entry was written: the sweep takes them all.
    let start_xid: String = d
        .ctl
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("a start")
        .get(0);
    let plan = build::one_to_one::OneToOnePlan::load(d.pool(), "a2")
        .await
        .expect("load the plan")
        .expect("a2 is a 1-1 build shape");
    let mut client = d.pool().get().await.expect("pool");
    let txn = client.transaction().await.expect("begin");
    let outcome = build::one_to_one::sweep_batch(&txn, &plan, &start_xid, None, 100)
        .await
        .expect("a sweep batch");
    txn.commit().await.expect("commit the sweep");
    assert_eq!(
        outcome.rederived, 3,
        "every entry, the NULL-keyed ones included"
    );
    assert_eq!(
        d.rows(A2_ACTUAL).await,
        d.rows("select g, h, n, t + 1 from public.agg order by g, h")
            .await,
        "the reader equals its source again"
    );
}

/// The direct build (`backfill_definition`, a build outside the chunk
/// queue) writes the `NULL`-keyed rows too, in a pass of their own after its
/// ranges, which never reach them.
#[tokio::test]
async fn the_direct_build_writes_the_null_keyed_rows() {
    let mut d = Driver::start(SRC_DDL, &src_columns(), &[AGG], &[SRC]).await;
    step(
        &mut d,
        "insert into public.src values (1, 1, null, 1), (2, null, null, 2), (3, 2, 2, 3), \
                                       (4, null, 5, 4)",
    )
    .await;
    // Registered and its target created; nothing builds it but the call
    // below.
    install(&d, A2, &agg_columns()).await;
    let def = trellis::defs::parse(A2).expect("parse");
    let columns: HashMap<String, ValueType> = agg_columns()
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect();
    trellis::defs::backfill_definition(d.pool(), &def, "public", "public.agg", &columns)
        .await
        .expect("the direct build");
    assert_eq!(d.rows(A2_ACTUAL).await, d.rows(A2_ORACLE).await);
}

/// An aggregate over the reader, grouped by a field: a `NULL`-keyed row's
/// moves between its groups (its count changes, or its group goes extinct
/// upstream) reach the rollup as any other row's do.
#[tokio::test]
async fn a_rollup_over_the_reader_follows_its_null_keyed_rows() {
    let mut d = start(None).await;
    install(
        &d,
        "TRANSFORM roll FROM public.a2 GROUP BY n WITHOUT SOURCE INDEX SELECT COUNT(*) AS groups",
        &[("g", int4()), ("h", int4()), ("n", int8()), ("t1", int8())],
    )
    .await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_live(&d).await;
    const ROLL: &str = "select n, groups from public.roll order by n";
    const ORACLE: &str = "select n, count(*) from (select count(*) as n from public.src \
                          group by g, h) s group by n order by n";
    for sql in [
        "insert into public.src values (1, 1, null, 1), (2, null, null, 1), (3, 1, 1, 1)",
        "insert into public.src values (4, 1, null, 1), (5, null, null, 1)",
        "insert into public.src values (6, null, null, 1)",
        "delete from public.src where id = 4",
        "delete from public.src where g is null and h is null",
        "update public.src set h = 2 where id = 1",
    ] {
        step(&mut d, sql).await;
        assert_equal(&mut d, ROLL, ORACLE).await;
    }
}
