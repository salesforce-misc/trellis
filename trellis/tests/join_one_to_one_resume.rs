//! A 1-1 `JOIN`'s whole-transform `PAUSE` and `RESUME`, end to end (ADR-0021,
//! #1514): changes to both tables while the transform is paused, the target
//! kept readable across the resume, the rebuild that follows and its orphan
//! pass, and the resumes that refuse.
//!
//! `join_one_to_one_build.rs` holds the resume's mechanics (the sweep's
//! orphan pass, its interleavings with a page, the plan job's cursor), and
//! `join_one_to_one_producers.rs` a `RESUME` of an aggregate used as `a`. This
//! file holds the scenarios a user sees: a resume after every kind of change
//! on both sides (a key move of `b` among them), with the oracle at every
//! step, over plain tables and over two 1-1 targets, and the refusals that
//! leave the definition paused with nothing recreated.
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`;
//! nothing polls or sleeps.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;

use drain_driver::Driver;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::integer::IntWidth;
use trellis::{Trellis, TrellisOptions};

const DDL: &str = "create table public.a (id text primary key, k integer, r integer, s text); \
                   create table public.b (bid bigint primary key, k integer, img integer, \
                       k0 text); \
                   create unique index b_k_unique on public.b (k); \
                   create index on public.a (k)";

/// Twenty `a` rows on keys 0 to 60 (every fifth on `NULL`), six `b` rows on
/// keys 10 to 60: some `a` rows have no match.
const SEED: &str = "insert into public.a \
                        select 'a' || lpad(i::text, 2, '0'), \
                               case when i % 5 = 0 then null else (i % 7) * 10 end, \
                               i, 's' || i \
                        from generate_series(1, 20) i; \
                    insert into public.b select i, i * 10, i * 100, 'k' || i \
                        from generate_series(1, 6) i";

/// The dev knob is process-wide, so these tests run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Left,
    Inner,
}

const KINDS: [Kind; 2] = [Kind::Left, Kind::Inner];

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Left => "jl",
            Kind::Inner => "ji",
        }
    }

    fn word(self) -> &'static str {
        match self {
            Kind::Left => "LEFT",
            Kind::Inner => "INNER",
        }
    }

    fn target(self) -> String {
        format!("public.{}", self.name())
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

async fn trellis(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
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

async fn pause(d: &Driver, kinds: &[Kind]) {
    let trellis = trellis(d).await;
    for kind in kinds {
        trellis
            .apply(&format!("PAUSE TRANSFORM {}", kind.name()))
            .await
            .expect("pause");
    }
}

/// Resumes `kinds` and starts the builds the resumes schedule, without
/// running them.
async fn resume(d: &Driver, kinds: &[Kind]) {
    let trellis = trellis(d).await;
    for kind in kinds {
        trellis
            .apply(&format!("RESUME TRANSFORM {}", kind.name()))
            .await
            .expect("resume");
    }
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the resumed builds");
}

// ----------------------------------------------- over two plain source tables

const COLUMNS: [(&str, ValueType); 4] = [
    ("id", ValueType::Text),
    ("k", ValueType::Integer(IntWidth::Int4)),
    ("r", ValueType::Integer(IntWidth::Int4)),
    ("s", ValueType::Text),
];

/// `kinds` over the seeded tables, built and live.
async fn start(kinds: &[Kind]) -> Driver {
    set_enabled(true);
    let definitions: Vec<String> = kinds
        .iter()
        .map(|kind| {
            format!(
                "TRANSFORM {} FROM public.a {} JOIN public.b ON a.k = b.k \
                 SELECT a.r + COALESCE(b.img, 0) AS total, b.k0 AS k0, a.s AS s",
                kind.name(),
                kind.word()
            )
        })
        .collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    Driver::start(
        &format!("{DDL}; {SEED}"),
        &COLUMNS,
        &definitions,
        &["public.a", "public.b"],
    )
    .await
}

/// `kind`'s target as a reader sees it.
async fn target_rows(d: &Driver, kind: Kind) -> Vec<String> {
    d.rows(&format!(
        "select id, total, k0, s from {} order by id",
        kind.target()
    ))
    .await
}

/// The join `kind`'s target holds when it is exact.
async fn live_join(d: &Driver, kind: Kind) -> Vec<String> {
    d.rows(&format!(
        "select a.id, a.r + coalesce(b.img, 0) as total, b.k0, a.s \
         from public.a a {} join public.b b on a.k = b.k order by a.id",
        kind.word()
    ))
    .await
}

/// Runs the pending builds, then asserts `kind`'s target equals the live
/// join, `R` holds `a`'s rows, `M` holds `b`'s and `self_check` is clean.
async fn assert_exact(d: &mut Driver, kind: Kind, step: &str) {
    trellis::staging::build::settle_builds_with(d.pool(), 3).await;
    d.settle().await;
    assert_eq!(
        target_rows(d, kind).await,
        live_join(d, kind).await,
        "{kind:?} target (left) differs from the live join (right) after {step}"
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &kind.target())
        .await
        .expect("self_check");
    assert!(diverging.is_empty(), "{kind:?} after {step}: {diverging:?}");
    let id = definition_id(d, &kind.target()).await;
    let r = d
        .rows(&format!(
            "select __from_key, k, r as c_r, s from {DEFAULT_SCHEMA}.join1_{id}_r \
             where not __tombstone order by 1"
        ))
        .await;
    let a = d
        .rows("select id, k, r as c_r, s from public.a order by 1")
        .await;
    assert_eq!(r, a, "{kind:?} R (left) differs from a after {step}");
    let m = d
        .rows(&format!(
            "select __from_key, k, img, k0 from {DEFAULT_SCHEMA}.join1_{id}_m \
             where not __tombstone order by 1"
        ))
        .await;
    let b = d
        .rows("select bid::text, k, img, k0 from public.b order by 1")
        .await;
    assert_eq!(m, b, "{kind:?} M (left) differs from b after {step}");
    assert_eq!(
        status(d, &kind.target()).await,
        "live",
        "{kind:?} after {step}"
    );
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
}

/// The changes of every kind a paused 1-1 `JOIN` misses, one drained step
/// each: `a` rows inserted (on a key `b` has, on one it hasn't, on `NULL`),
/// changed, moved onto another matching key, onto `NULL` and onto a key `b`
/// lacks, and deleted; `b` rows changed, inserted, deleted, moved onto a key
/// no `b` row held (a key move that keeps `ON` unique), and two moved past
/// each other in one transaction (`b3` takes `b4`'s key, `b4` takes `b3`'s).
const WHILE_PAUSED: [&str; 8] = [
    "insert into public.a values ('a21', 20, 21, 's21'), ('a22', 15, 22, 's22'), \
                                 ('a23', null, 23, 's23')",
    "update public.a set r = r + 1000 where id in ('a07', 'a08')",
    "update public.a set k = 40 where id = 'a01'; \
     update public.a set k = null where id = 'a02'; \
     update public.a set k = 15 where id = 'a03'",
    "delete from public.a where id in ('a04', 'a06', 'a20')",
    "update public.b set img = 150 where bid = 1; \
     insert into public.b values (7, 0, 700, 'k7'), (8, 15, 800, 'k8')",
    "update public.b set k = 25 where bid = 2",
    "begin; \
     update public.b set k = -1 where bid = 3; \
     update public.b set k = 30 where bid = 4; \
     update public.b set k = 40 where bid = 3; \
     commit",
    "delete from public.b where bid = 5",
];

/// A `RESUME` after every kind of change on both tables: the target stays as
/// the pause left it and readable, whatever it holds, through the resume, and
/// the rebuild brings it to the live join, the target rows of the `a` rows
/// deleted while paused going with it (the sweep's orphan pass). Then a change
/// of each table past the resume is applied by its page, and a second pause
/// and resume with nothing in between changes nothing.
#[tokio::test]
async fn a_resume_after_every_kind_of_change_to_both_tables_is_exact() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(&[kind]).await;
        assert_exact(&mut d, kind, "the build").await;
        let at_pause = target_rows(&d, kind).await;
        pause(&d, &[kind]).await;
        for sql in WHILE_PAUSED {
            step(&mut d, sql).await;
        }
        assert_eq!(
            target_rows(&d, kind).await,
            at_pause,
            "{kind:?}: a paused target takes no change"
        );
        assert_ne!(
            live_join(&d, kind).await,
            at_pause,
            "{kind:?}: the join moved"
        );

        resume(&d, &[kind]).await;
        assert_eq!(
            status(&d, &kind.target()).await,
            "backfilling",
            "{kind:?}: the resume starts the build"
        );
        assert_eq!(
            target_rows(&d, kind).await,
            at_pause,
            "{kind:?}: the resume keeps the target, readable and as it was"
        );
        let resumed = target_rows(&d, kind).await;
        for gone in ["a04", "a06"] {
            assert!(
                resumed
                    .iter()
                    .any(|row| row.starts_with(&format!("({gone},"))),
                "{kind:?}: {gone} is in the target the orphan pass is yet to reach"
            );
        }
        assert_exact(&mut d, kind, "the resume").await;

        step(
            &mut d,
            "update public.a set r = r + 1 where id in ('a01', 'a21'); \
             insert into public.a values ('a24', 25, 24, 's24'); \
             update public.b set img = img + 5 where bid in (2, 3, 7)",
        )
        .await;
        assert_exact(&mut d, kind, "a change after the resume").await;
        step(&mut d, "delete from public.b where bid = 3").await;
        assert_exact(&mut d, kind, "a delete of b after the resume").await;

        pause(&d, &[kind]).await;
        resume(&d, &[kind]).await;
        assert_exact(&mut d, kind, "a second pause and resume").await;
    }
}

/// LEFT and INNER over the same tables, paused and resumed together: each
/// definition's resume recreates its own hidden tables and orphan pass, and
/// the two builds take the pair's lock rows in one order.
#[tokio::test]
async fn a_resume_of_both_joins_over_the_same_tables_is_exact() {
    let _serial = SERIAL.lock().await;
    let mut d = start(&KINDS).await;
    pause(&d, &KINDS).await;
    for sql in WHILE_PAUSED {
        step(&mut d, sql).await;
    }
    resume(&d, &KINDS).await;
    for kind in KINDS {
        assert_exact(&mut d, kind, "the resume of both").await;
    }
}

// ------------------------------------------------------------------ refusals

/// A resume that refuses leaves the definition paused and changes nothing
/// of what it holds: not the target, `R` or `M`. It names the table, and once
/// the cause is undone the resume goes through and the rebuild is exact.
/// (1) `b`'s `ON` columns lose the unique index they rested on, (2) `a`'s
/// primary key is redefined, which the target, keyed by `a`'s key as define
/// found it, cannot follow.
#[tokio::test]
async fn a_refused_resume_leaves_the_definition_paused_and_untouched() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(&[kind]).await;
        let id = definition_id(&d, &kind.target()).await;
        let hidden = format!(
            "select (select count(*) from {DEFAULT_SCHEMA}.join1_{id}_r), \
                    (select count(*) from {DEFAULT_SCHEMA}.join1_{id}_m), \
                    (select count(*) from {DEFAULT_SCHEMA}.join1_{id}_j)"
        );
        pause(&d, &[kind]).await;
        step(&mut d, "update public.b set img = 999 where bid = 1").await;
        let held = (target_rows(&d, kind).await, d.rows(&hidden).await);
        assert_ne!(
            held.1,
            ["(0,0,0)"],
            "{kind:?}: the hidden tables hold the build"
        );
        let trellis = trellis(&d).await;

        // (1) b's ON columns are no longer unique.
        write(&d, "drop index public.b_k_unique").await;
        let err = trellis
            .apply(&format!("RESUME TRANSFORM {}", kind.name()))
            .await
            .expect_err("the resume revalidates b's uniqueness");
        let message = err.to_string();
        assert!(
            message.contains("unique") && message.contains("public.b"),
            "{kind:?}: {message}"
        );
        assert_eq!(status(&d, &kind.target()).await, "paused", "{kind:?}");
        assert_eq!((target_rows(&d, kind).await, d.rows(&hidden).await), held);
        write(&d, "create unique index b_k_unique on public.b (k)").await;

        // (2) a's key changed.
        write(
            &d,
            "alter table public.a drop constraint a_pkey, add primary key (id, s)",
        )
        .await;
        let err = trellis
            .apply(&format!("RESUME TRANSFORM {}", kind.name()))
            .await
            .expect_err("the resume checks a's key");
        let message = err.to_string();
        assert!(
            message.contains("primary key of public.a is now (id, s)"),
            "{kind:?}: {message}"
        );
        assert_eq!(status(&d, &kind.target()).await, "paused", "{kind:?}");
        assert_eq!((target_rows(&d, kind).await, d.rows(&hidden).await), held);
        write(
            &d,
            "alter table public.a drop constraint a_pkey, add primary key (id)",
        )
        .await;

        // The causes undone, a change since the pause is still rebuilt.
        step(&mut d, "delete from public.a where id = 'a03'").await;
        resume(&d, &[kind]).await;
        assert_exact(&mut d, kind, "the resume once the causes are undone").await;
    }
}

// ------------------------------------------- over two other 1-1 transforms' targets

/// A 1-1 `JOIN` over two 1-1 targets: `ta` of `asrc` as `a` and `tb` of `bsrc`
/// as `b`, joined on `ta.k = tb.id` (`tb`'s whole primary key).
async fn start_chained(kinds: &[Kind]) -> Driver {
    set_enabled(true);
    let mut d = Driver::start(
        "create table public.asrc (id integer primary key, k integer, x integer); \
         create table public.bsrc (id integer primary key, w integer, tag text); \
         insert into public.asrc select i, (i % 6) * 10, i from generate_series(1, 12) i; \
         insert into public.bsrc select i * 10, i * 100, 't' || i from generate_series(1, 4) i",
        &[
            ("k", int4()),
            ("x", int4()),
            ("w", int4()),
            ("tag", ValueType::Text),
        ],
        &[
            "TRANSFORM ta FROM public.asrc SELECT k AS k, x AS x",
            "TRANSFORM tb FROM public.bsrc SELECT w AS w, tag AS tag",
        ],
        &["public.asrc", "public.bsrc"],
    )
    .await;
    let columns = HashMap::from([
        ("total".to_string(), int4()),
        ("tag".to_string(), ValueType::Text),
    ]);
    for kind in kinds {
        trellis::defs::install_definition(
            d.pool(),
            &format!(
                "TRANSFORM {} FROM public.ta {} JOIN public.tb ON ta.k = tb.id \
                 SELECT ta.x + COALESCE(tb.w, 0) AS total, tb.tag AS tag",
                kind.name(),
                kind.word()
            ),
            &columns,
            "public",
        )
        .await
        .expect("define the join over the 1-1 targets");
    }
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    d
}

/// Asserts each of `kinds`' targets equals the join of the two sources,
/// after the pending builds, with every definition live and `self_check`
/// clean.
async fn assert_chained_exact(d: &mut Driver, kinds: &[Kind], step: &str) {
    trellis::staging::build::settle_builds_with(d.pool(), 3).await;
    d.settle().await;
    let statuses = d
        .rows("select status::text from transform_definitions order by id")
        .await;
    assert!(
        statuses.iter().all(|status| status == "(live)"),
        "{statuses:?} after {step}"
    );
    for kind in kinds {
        let word = kind.word();
        assert_eq!(
            d.rows(&format!(
                "select id, total, tag from {} order by id",
                kind.target()
            ))
            .await,
            d.rows(&format!(
                "select a.id, a.x + coalesce(b.w, 0), b.tag from public.asrc a \
                 {word} join public.bsrc b on a.k = b.id order by a.id"
            ))
            .await,
            "{kind:?} after {step}"
        );
        let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &kind.target())
            .await
            .expect("self_check");
        assert!(diverging.is_empty(), "{kind:?} after {step}: {diverging:?}");
    }
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
}

/// A join over two 1-1 targets, paused while both sources change (the two
/// upstream transforms stay live and keep their targets exact, which the
/// paused join misses): rows inserted, changed, moved onto another key of
/// `tb` and onto none, deleted, and `tb` rows inserted, changed and deleted.
/// The resume rebuilds from the two targets, and the sweep removes the rows
/// of the `a` rows `ta` lost.
#[tokio::test]
async fn a_resume_of_a_join_over_two_one_to_one_targets_is_exact() {
    let _serial = SERIAL.lock().await;
    let mut d = start_chained(&KINDS).await;
    assert_chained_exact(&mut d, &KINDS, "the build").await;
    pause(&d, &KINDS).await;
    for sql in [
        "insert into public.asrc values (13, 20, 13), (14, 5, 14), (15, null, 15)",
        "update public.asrc set x = x + 100 where id in (1, 2)",
        "update public.asrc set k = 30 where id = 3; update public.asrc set k = 7 where id = 4",
        "delete from public.asrc where id in (5, 6, 12)",
        "update public.bsrc set w = 150 where id = 10; \
         insert into public.bsrc values (0, 5, 'zero'), (5, 55, 'five'); \
         delete from public.bsrc where id = 40",
    ] {
        step(&mut d, sql).await;
    }
    let statuses = d
        .rows("select target_table, status::text from transform_definitions order by id")
        .await;
    assert_eq!(
        statuses,
        [
            "(public.ta,live)",
            "(public.tb,live)",
            "(public.jl,paused)",
            "(public.ji,paused)"
        ]
    );
    resume(&d, &KINDS).await;
    assert_chained_exact(&mut d, &KINDS, "the resume").await;
    step(
        &mut d,
        "update public.asrc set k = 10 where id = 13; update public.bsrc set tag = 'ten' where id = 10",
    )
    .await;
    assert_chained_exact(&mut d, &KINDS, "a change after the resume").await;
}
