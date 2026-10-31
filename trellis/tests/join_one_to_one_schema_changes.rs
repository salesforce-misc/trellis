//! A 1-1 `JOIN` across a schema change of either table (ADR-0021, #1403), as
//! an aggregate `JOIN` handles each (ADR-0019, docs/transforms.md): a column a
//! field reads widened, `a`'s key widened, an `ON` column retyped on one side
//! or both, a column read dropped, renamed or retyped to a type the transform
//! evaluates differently, a table renamed away or dropped, and a table
//! attached as a partition. Each pauses the whole transform with a
//! `capture_failure` naming the table and column, and `RESUME` brings Trellis's
//! copies to the types define would give them now (the target's through the
//! staging worker, `R`, `M` and `J` created again) and rebuilds, after which
//! the target equals the live join. Over plain tables (`LEFT` and `INNER`), a
//! chained 1-1 `a` side, a chained 1-1 `b` side, and an aggregate's target as
//! `a` keyed by a nullable group column.
//!
//! Every test drives the pipeline and the staging worker's pass by hand
//! through `support/drain_driver.rs`; nothing polls or sleeps.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;

use drain_driver::Driver;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::join_one_to_one::set_enabled;
use trellis::defs::{TransformStatus, ValueType};
use trellis::integer::IntWidth;
use trellis::{Trellis, TrellisError, TrellisOptions};

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

/// One whole staging-worker pass: capture (the schema-change checks among
/// it), the requested resumes, then the start of ready builds.
async fn pass(d: &mut Driver) {
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
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

/// `table.column`'s type, as `format_type` renders it.
async fn column_type(d: &Driver, table: &str, column: &str) -> String {
    d.ctl
        .query_one(
            "select pg_catalog.format_type(atttypid, atttypmod) from pg_catalog.pg_attribute \
             where attrelid = pg_catalog.to_regclass($1) and attname = $2",
            &[&table, &column],
        )
        .await
        .unwrap_or_else(|e| panic!("{table}.{column}: {e}"))
        .get(0)
}

/// The type of `column` in definition `target`'s hidden table `part`
/// (`r`, `m` or `j`).
async fn hidden_type(d: &Driver, target: &str, part: &str, column: &str) -> String {
    let id = definition_id(d, target).await;
    column_type(d, &format!("{DEFAULT_SCHEMA}.join1_{id}_{part}"), column).await
}

/// The reason `target` is paused with, asserting it is paused for a capture
/// failure on `table` about `columns`.
async fn paused_for(d: &Driver, target: &str, table: &str, columns: &[&str]) -> String {
    let bare = target.split_once('.').map_or(target, |(_, bare)| bare);
    let reported = trellis(d)
        .await
        .status(bare)
        .await
        .expect("status")
        .expect(target);
    assert_eq!(reported.status, TransformStatus::Paused, "{target}");
    let failure = reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target}'s reason is reported"));
    assert_eq!(failure.source_table, table, "{target}: {}", failure.error);
    assert_eq!(
        failure.columns,
        columns.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
        "{target}: {}",
        failure.error
    );
    failure.error
}

/// `RESUME TRANSFORM target`, which must go through.
async fn resume(d: &Driver, target: &str) {
    trellis(d)
        .await
        .apply(&format!("RESUME TRANSFORM {target}"))
        .await
        .unwrap_or_else(|err| panic!("resume {target}: {err}"));
}

/// `RESUME TRANSFORM target`, which must be refused: returns the message.
async fn resume_refused(d: &Driver, target: &str) -> String {
    match trellis(d)
        .await
        .apply(&format!("RESUME TRANSFORM {target}"))
        .await
    {
        Err(
            err @ TrellisError::Apply(trellis::staging::apply::ApplyError::ResumeRefused { .. }),
        ) => err.to_string(),
        other => panic!("expected the resume of {target} to be refused, got {other:?}"),
    }
}

/// Whether `target`'s resume waits on the staging worker to re-type its
/// copies: it is paused, and its reason says so. Returns the reason.
async fn assert_retyping(d: &Driver, target: &str) -> String {
    assert_eq!(status(d, target).await, "paused", "{target}");
    let error: String = d
        .ctl
        .query_one(
            "select f.error from capture_failures f join transform_definitions d \
             on d.id = f.transform_id where d.target_table = $1",
            &[&target],
        )
        .await
        .expect("the reason")
        .get(0);
    assert!(error.starts_with("resuming:"), "{error}");
    error
}

// ----------------------------------------------- over two plain source tables

const DDL: &str = "create table public.a (id integer primary key, k integer, r integer, \
                       s text); \
                   create table public.b (bid integer primary key, k integer, img integer, \
                       k0 text); \
                   create unique index b_k_unique on public.b (k); \
                   create index on public.a (k)";

/// Twelve `a` rows on keys 0 to 30 (every fifth on `NULL`), with `s` holding
/// digits; three `b` rows on keys 10 to 30: some `a` rows have no match.
const SEED: &str = "insert into public.a \
                        select i, case when i % 5 = 0 then null else (i % 4) * 10 end, \
                               i, i::text \
                        from generate_series(1, 12) i; \
                    insert into public.b select i, i * 10, i * 100, 'k' || i \
                        from generate_series(1, 3) i";

const COLUMNS: [(&str, ValueType); 4] = [
    ("id", ValueType::Integer(IntWidth::Int4)),
    ("k", ValueType::Integer(IntWidth::Int4)),
    ("r", ValueType::Integer(IntWidth::Int4)),
    ("s", ValueType::Text),
];

/// `kind` over the seeded tables, built and live.
async fn start(kind: Kind) -> Driver {
    set_enabled(true);
    let definition = format!(
        "TRANSFORM {} FROM public.a {} JOIN public.b ON a.k = b.k \
         SELECT a.r + COALESCE(b.img, 0) AS total, b.k0 AS k0, a.s AS s",
        kind.name(),
        kind.word()
    );
    Driver::start(
        &format!("{DDL}; {SEED}"),
        &COLUMNS,
        &[&definition],
        &["public.a", "public.b"],
    )
    .await
}

/// Runs a staging-worker pass and the pending builds, then asserts `kind`'s target equals the live
/// join, by its rows and by `self_check`, and that the transform is live.
async fn assert_exact(d: &mut Driver, kind: Kind, step: &str) {
    pass(d).await;
    trellis::staging::build::settle_builds_with(d.pool(), 3).await;
    d.settle().await;
    let target = d
        .rows(&format!(
            "select t.id::text, t.total::text, t.k0::text, t.s::text from {} t order by t.id",
            kind.target()
        ))
        .await;
    let join = d
        .rows(&format!(
            "select a.id::text, (a.r + coalesce(b.img, 0))::text, b.k0::text, a.s::text \
             from public.a a {} join public.b b on a.k = b.k order by a.id",
            kind.word()
        ))
        .await;
    let state = d
        .rows(
            "select d.status, f.error from transform_definitions d \
               left join capture_failures f on f.transform_id = d.id",
        )
        .await;
    assert_eq!(
        target, join,
        "{kind:?} target (left) differs from the live join (right) after {step}: {state:?}"
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &kind.target())
        .await
        .expect("self_check");
    assert!(diverging.is_empty(), "{kind:?} after {step}: {diverging:?}");
    assert_eq!(
        status(d, &kind.target()).await,
        "live",
        "{kind:?} after {step}"
    );
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
}

/// A column one field reads widened, on either table (`a.r`, `b.img`, both
/// read by `total`, an `integer`): the pass pauses the transform, naming the
/// target's column it outgrew and every column it is typed from with its
/// type now. Nothing records an expression's input types, so, as for an
/// aggregate `JOIN`'s field over both tables, the pass of either table may be
/// the one that finds it, and the pause names that table. `RESUME` waits for
/// the staging worker to re-type `total` to `bigint`, then rebuilds, and a
/// value past `integer` lands on both sides. A pass after it pauses nothing.
#[tokio::test]
async fn a_widened_column_a_field_reads_pauses_and_resume_re_types_the_target() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        for (table, column, big) in [
            (
                "public.a",
                "r",
                "update public.a set r = 3000000000 where id = 1",
            ),
            (
                "public.b",
                "img",
                "update public.b set img = 3000000000 where bid = 1",
            ),
        ] {
            let mut d = start(kind).await;
            let target = kind.target();
            write(
                &d,
                &format!("alter table {table} alter column {column} type bigint"),
            )
            .await;
            pass(&mut d).await;
            assert_eq!(status(&d, &target).await, "paused", "{kind:?} {table}");
            let error: String = d
                .ctl
                .query_one("select error from capture_failures", &[])
                .await
                .expect("the reason")
                .get(0);
            assert!(
                error.contains(&format!("{target}.total (integer, now bigint)"))
                    && error.contains(&format!("{table}.{column} (bigint)"))
                    && error.contains("Resume the definition"),
                "{kind:?} {table}.{column}: {error}"
            );

            resume(&d, kind.name()).await;
            let error = assert_retyping(&d, &target).await;
            assert!(
                error.contains(&format!("{target}.total from integer to bigint")),
                "{error}"
            );
            pass(&mut d).await;
            assert_eq!(column_type(&d, &target, "total").await, "bigint");
            let part = if table == "public.a" { "r" } else { "m" };
            assert_eq!(
                hidden_type(&d, &target, part, column).await,
                "bigint",
                "{part} is created again at the column's new type"
            );
            assert_exact(&mut d, kind, "the resume").await;
            step(&mut d, big).await;
            assert_exact(&mut d, kind, "a value past integer").await;
            pass(&mut d).await;
            assert_eq!(status(&d, &target).await, "live", "{kind:?}");
        }
    }
}

/// A column only `R` or `M` holds a copy of at its own type, widened: the
/// fields cast it to `numeric`, which holds every `bigint`, so nothing of the
/// target is outgrown, but `R.r` and `M.img` are. The pass pauses the
/// transform naming the hidden column, and `RESUME` creates `R` and `M`
/// again at `bigint`, with nothing to re-type, so a value past `integer`
/// lands rather than failing its write.
#[tokio::test]
async fn a_widened_column_only_a_hidden_table_copies_pauses_and_resume_creates_it_again() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        for (table, column, part) in [("public.a", "r", "r"), ("public.b", "img", "m")] {
            set_enabled(true);
            let mut d = Driver::start(
                &format!("{DDL}; {SEED}"),
                &COLUMNS,
                &[&format!(
                    "TRANSFORM {} FROM public.a {} JOIN public.b ON a.k = b.k \
                     SELECT CAST(a.r AS numeric) AS rn, CAST(b.img AS numeric) AS bn",
                    kind.name(),
                    kind.word()
                )],
                &["public.a", "public.b"],
            )
            .await;
            let target = kind.target();
            write(
                &d,
                &format!("alter table {table} alter column {column} type bigint"),
            )
            .await;
            pass(&mut d).await;
            let error = paused_for(&d, &target, table, &[column]).await;
            assert!(
                error.contains("join1_") && error.contains(&format!(".{column} (integer")),
                "{kind:?} {table}: {error}"
            );
            resume(&d, kind.name()).await;
            assert_eq!(status(&d, &target).await, "waiting_to_backfill");
            assert_eq!(hidden_type(&d, &target, part, column).await, "bigint");
            step(
                &mut d,
                &format!(
                    "update {table} set {column} = 3000000000 where {} = 1",
                    if table == "public.a" { "id" } else { "bid" }
                ),
            )
            .await;
            pass(&mut d).await;
            trellis::staging::build::settle_builds_with(d.pool(), 3).await;
            d.settle().await;
            assert_eq!(status(&d, &target).await, "live", "{kind:?} {table}");
            assert_eq!(
                d.rows(&format!(
                    "select t.id::text, t.rn::text, t.bn::text from {target} t order by t.id"
                ))
                .await,
                d.rows(&format!(
                    "select a.id::text, a.r::text as rn, b.img::text from public.a a {} join public.b b \
                     on a.k = b.k order by a.id",
                    kind.word()
                ))
                .await,
                "{kind:?} {table}"
            );
        }
    }
}

/// A field that names one column is a copy of that column, at the column's
/// own type with no modifier, whatever define's inference would call it: an
/// enum, a `varchar(5)` and a `numeric(6,2)`. A pass and a `PAUSE`/`RESUME`
/// with nothing changed re-type none of them (the resume goes straight to its
/// rebuild), and the target keeps the enum.
#[tokio::test]
async fn a_copy_of_one_column_keeps_its_type_through_a_resume() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        set_enabled(true);
        let mut d = Driver::start(
            "create type public.mood as enum ('sad', 'ok', 'happy'); \
             create table public.a (id integer primary key, k integer, \
                 amount numeric(6,2)); \
             create table public.b (bid integer primary key, k integer unique, \
                 m public.mood, tag varchar(5)); \
             create index on public.a (k); \
             insert into public.a select i, i % 3, i * 1.25 from generate_series(1, 6) i; \
             insert into public.b values (1, 1, 'ok', 'one'), (2, 2, 'happy', 'two')",
            &[("k", int4()), ("amount", ValueType::Numeric)],
            &[&format!(
                "TRANSFORM {} FROM public.a {} JOIN public.b ON a.k = b.k \
                 SELECT b.m AS m, b.tag AS tag, a.amount AS amount",
                kind.name(),
                kind.word()
            )],
            &["public.a", "public.b"],
        )
        .await;
        let target = kind.target();
        pass(&mut d).await;
        assert_eq!(status(&d, &target).await, "live", "{kind:?}");
        let trellis = trellis(&d).await;
        trellis
            .apply(&format!("PAUSE TRANSFORM {}", kind.name()))
            .await
            .expect("pause");
        resume(&d, kind.name()).await;
        assert_eq!(
            status(&d, &target).await,
            "waiting_to_backfill",
            "{kind:?}: nothing to re-type"
        );
        for (column, ty) in [
            ("m", "mood"),
            ("tag", "character varying"),
            ("amount", "numeric"),
        ] {
            assert_eq!(column_type(&d, &target, column).await, ty, "{kind:?}");
        }
        pass(&mut d).await;
        trellis::staging::build::settle_builds_with(d.pool(), 3).await;
        d.settle().await;
        assert_eq!(status(&d, &target).await, "live", "{kind:?}");
        assert_eq!(
            d.rows(&format!(
                "select t.id::text, t.m::text, t.tag::text, t.amount::text \
                 from {target} t order by t.id"
            ))
            .await,
            d.rows(&format!(
                "select a.id::text, b.m::text, b.tag::text, a.amount::text from public.a a \
                 {} join public.b b on a.k = b.k order by a.id",
                kind.word()
            ))
            .await,
            "{kind:?}"
        );
    }
}

/// `a`'s primary key widened: the target's key, a copy of it, is outgrown.
/// The pass pauses the transform naming `public.a` and `id`; `RESUME` re-types
/// the target's key and rebuilds, and an `a` row keyed past `integer` joins.
#[tokio::test]
async fn a_widened_key_of_a_pauses_and_resume_re_types_the_target_key() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        let target = kind.target();
        write(&d, "alter table public.a alter column id type bigint").await;
        pass(&mut d).await;
        let error = paused_for(&d, &target, "public.a", &["id"]).await;
        assert!(error.contains(&format!("{target}.id (integer)")), "{error}");
        resume(&d, kind.name()).await;
        assert_retyping(&d, &target).await;
        pass(&mut d).await;
        assert_eq!(column_type(&d, &target, "id").await, "bigint");
        assert_exact(&mut d, kind, "the resume").await;
        step(
            &mut d,
            "insert into public.a values (3000000000, 10, 5, '5'), (3000000001, null, 6, '6')",
        )
        .await;
        assert_exact(&mut d, kind, "an a row keyed past integer").await;
    }
}

/// An `ON` column retyped on one side only (`integer` to `bigint`) breaks the
/// pair's one type: the pass pauses the transform naming that table and
/// column, and `RESUME` refuses, naming the pair, until the other side
/// matches. Then `RESUME` creates `R`, `M` and `J` again at the new key type
/// (`J`'s key type changed) and rebuilds, keeping the target, and keys past
/// `integer` join. Either side first.
#[tokio::test]
async fn an_on_column_retyped_on_one_side_pauses_until_the_pair_matches_again() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        for (first, second) in [("public.a", "public.b"), ("public.b", "public.a")] {
            let mut d = start(kind).await;
            let target = kind.target();
            let held = d.rows(&format!("select * from {target} order by id")).await;
            write(
                &d,
                &format!("alter table {first} alter column k type bigint"),
            )
            .await;
            pass(&mut d).await;
            let error = paused_for(&d, &target, first, &["k"]).await;
            assert!(error.contains("don't match"), "{kind:?} {first}: {error}");
            let refused = resume_refused(&d, kind.name()).await;
            assert!(refused.contains("k"), "{refused}");
            assert_eq!(status(&d, &target).await, "paused");
            assert_eq!(
                d.rows(&format!("select * from {target} order by id")).await,
                held,
                "a refused resume leaves the target"
            );

            write(
                &d,
                &format!("alter table {second} alter column k type bigint"),
            )
            .await;
            resume(&d, kind.name()).await;
            for part in ["r", "m", "j"] {
                assert_eq!(
                    hidden_type(&d, &target, part, "k").await,
                    "bigint",
                    "{kind:?}: {part} is created again at the pair's new type"
                );
            }
            assert_exact(&mut d, kind, "the resume").await;
            step(
                &mut d,
                "insert into public.b values (9, 3000000000, 900, 'k9'); \
                 insert into public.a values (100, 3000000000, 100, '100')",
            )
            .await;
            assert_exact(&mut d, kind, "a key past integer").await;
        }
    }
}

/// Both `ON` columns widened before any pass: the pair still has one type, so
/// nothing refuses, but `R`, `M` and `J` hold the key as `integer`, which the
/// new keys outgrow. The pass pauses the transform naming the key column,
/// and `RESUME` creates them again at `bigint` (nothing of the target to
/// re-type) and rebuilds.
#[tokio::test]
async fn an_on_pair_widened_on_both_sides_pauses_and_resume_creates_j_again() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        let target = kind.target();
        write(
            &d,
            "alter table public.a alter column k type bigint; \
             alter table public.b alter column k type bigint",
        )
        .await;
        pass(&mut d).await;
        assert_eq!(status(&d, &target).await, "paused", "{kind:?}");
        let error: String = d
            .ctl
            .query_one("select error from capture_failures", &[])
            .await
            .expect("the reason")
            .get(0);
        assert!(error.contains("join1_"), "{kind:?}: {error}");
        resume(&d, kind.name()).await;
        assert_eq!(status(&d, &target).await, "waiting_to_backfill");
        for part in ["r", "m", "j"] {
            assert_eq!(hidden_type(&d, &target, part, "k").await, "bigint");
        }
        assert_exact(&mut d, kind, "the resume").await;
        step(
            &mut d,
            "insert into public.b values (9, 3000000000, 900, 'k9'); \
             insert into public.a values (100, 3000000000, 100, '100')",
        )
        .await;
        assert_exact(&mut d, kind, "a key past integer").await;
    }
}

/// A column a field reads renamed (`b.img`) or dropped (`a.s`) pauses the
/// transform naming that table and column. Once it is back, `RESUME`
/// rebuilds.
#[tokio::test]
async fn a_column_either_side_reads_renamed_or_dropped_pauses_naming_it() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        let target = kind.target();
        write(&d, "alter table public.b rename column img to img2").await;
        pass(&mut d).await;
        let error = paused_for(&d, &target, "public.b", &["img"]).await;
        assert!(error.contains("renamed or dropped"), "{error}");
        write(&d, "alter table public.b rename column img2 to img").await;
        step(&mut d, "update public.b set img = 7 where bid = 2").await;
        resume(&d, kind.name()).await;
        assert_exact(&mut d, kind, "the resume after a rename").await;

        write(&d, "alter table public.a drop column s").await;
        // A write marks it before any pass, and the drain pauses on it.
        step(&mut d, "update public.a set r = 50 where id = 2").await;
        let error = paused_for(&d, &target, "public.a", &["s"]).await;
        assert!(error.contains("renamed or dropped"), "{error}");
        write(
            &d,
            "alter table public.a add column s text; update public.a set s = id::text",
        )
        .await;
        pass(&mut d).await;
        resume(&d, kind.name()).await;
        // The first pass installs capture of the re-added column, which
        // holds the rebuild until the next.
        pass(&mut d).await;
        d.settle().await;
        assert_exact(&mut d, kind, "the resume after a drop").await;
    }
}

/// A column a field passes through retyped to another family (`a.s`, `text`
/// to `integer`), which the transform evaluates as another type: the pass
/// pauses it, naming the column, and `RESUME` re-types the target's copy and
/// rebuilds.
#[tokio::test]
async fn a_column_retyped_to_a_type_evaluated_differently_pauses_and_resume_rebuilds() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        let target = kind.target();
        write(
            &d,
            "alter table public.a alter column s type integer using s::integer",
        )
        .await;
        pass(&mut d).await;
        let error = paused_for(&d, &target, "public.a", &["s"]).await;
        assert!(error.contains("evaluates"), "{kind:?}: {error}");
        resume(&d, kind.name()).await;
        assert_retyping(&d, &target).await;
        pass(&mut d).await;
        assert_eq!(column_type(&d, &target, "s").await, "integer");
        assert_exact(&mut d, kind, "the resume").await;
        step(&mut d, "update public.a set s = 77 where id = 3").await;
        assert_exact(&mut d, kind, "a change after the resume").await;
    }
}

/// Either table renamed away, or dropped and created again, pauses the
/// transform with a disruption naming it. Put back, `RESUME` rebuilds from the
/// table now at that name.
#[tokio::test]
async fn either_table_renamed_away_or_dropped_pauses_the_transform() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        for table in ["a", "b"] {
            let mut d = start(kind).await;
            let target = kind.target();
            write(
                &d,
                &format!("alter table public.{table} rename to {table}_x"),
            )
            .await;
            pass(&mut d).await;
            assert_eq!(status(&d, &target).await, "paused", "{kind:?} {table}");
            let failure = d
                .rows("select source_table, kind from capture_failures")
                .await;
            assert_eq!(
                failure,
                [format!("(public.{table},disruption)")],
                "{kind:?} {table}"
            );
            write(
                &d,
                &format!("alter table public.{table}_x rename to {table}"),
            )
            .await;
            write(
                &d,
                "update public.a set r = r + 1 where id = 1; \
                 update public.b set img = img + 1 where bid = 1",
            )
            .await;
            resume(&d, kind.name()).await;
            pass(&mut d).await;
            assert_exact(&mut d, kind, "the resume after a rename").await;
        }
        let mut d = start(kind).await;
        let target = kind.target();
        write(
            &d,
            "drop table public.b; \
             create table public.b (bid integer primary key, k integer, img integer, \
                 k0 text); \
             create unique index b_k_unique on public.b (k); \
             insert into public.b values (1, 10, 1, 'one'), (4, 0, 4, 'four')",
        )
        .await;
        pass(&mut d).await;
        assert_eq!(status(&d, &target).await, "paused", "{kind:?}");
        resume(&d, kind.name()).await;
        pass(&mut d).await;
        assert_exact(&mut d, kind, "the resume after b was created again").await;
    }
}

/// A table attached as a partition of another pauses the transform (capture
/// can't see writes through the hierarchy). Detached, `RESUME` rebuilds.
#[tokio::test]
async fn a_side_attached_as_a_partition_pauses_the_transform() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        let target = kind.target();
        write(
            &d,
            "create table public.bp (bid integer not null, k integer, img integer, k0 text) \
                 partition by range (bid); \
             alter table public.bp attach partition public.b \
                 for values from (minvalue) to (maxvalue)",
        )
        .await;
        pass(&mut d).await;
        assert_eq!(status(&d, &target).await, "paused", "{kind:?}");
        let failure = d.rows("select source_table from capture_failures").await;
        assert_eq!(failure, ["(public.b)"], "{kind:?}");
        let refused = resume_refused(&d, kind.name()).await;
        assert!(refused.contains("public.b"), "{refused}");
        write(
            &d,
            "alter table public.bp detach partition public.b; \
             update public.b set img = 5 where bid = 3",
        )
        .await;
        resume(&d, kind.name()).await;
        pass(&mut d).await;
        assert_exact(&mut d, kind, "the resume once detached").await;
    }
}

// ------------------------------------------------ a chained 1-1 side as `a`

/// `ta` (a 1-1 target of `asrc`) as `a`, joined to the plain `b`. A widening
/// of `asrc.x` outgrows `ta.x`, so `ta` pauses; its `RESUME` re-types `ta.x`,
/// which outgrows the join's copy of it, `x`: the join is paused with `ta`'s
/// resume as its cause, and resumes on its own once `ta` is live, re-typing
/// `x` and rebuilding from `ta`.
#[tokio::test]
async fn a_widening_under_a_chained_a_side_follows_its_upstream_resume() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        set_enabled(true);
        let mut d = Driver::start(
            "create table public.asrc (id integer primary key, k integer, x integer); \
             create table public.b (bid integer primary key, k integer, w integer); \
             create unique index on public.b (k); \
             insert into public.asrc select i, (i % 4) * 10, i from generate_series(1, 10) i; \
             insert into public.b select i, i * 10, i * 100 from generate_series(1, 2) i",
            &[("k", int4()), ("x", int4())],
            &["TRANSFORM ta FROM public.asrc SELECT k AS k, x AS x"],
            &["public.asrc", "public.b"],
        )
        .await;
        trellis::defs::install_definition(
            d.pool(),
            &format!(
                "TRANSFORM jn FROM public.ta {} JOIN public.b ON ta.k = b.k \
                 SELECT ta.x AS x, b.w AS w",
                kind.word()
            ),
            &HashMap::from([("x".to_string(), int4()), ("w".to_string(), int4())]),
            "public",
        )
        .await
        .expect("define the join over ta");
        pass(&mut d).await;
        trellis::intake::harness::settle_builds(d.pool()).await;
        d.settle().await;
        chained_exact(&d, kind).await;

        write(&d, "alter table public.asrc alter column x type bigint").await;
        pass(&mut d).await;
        assert_eq!(status(&d, "public.ta").await, "paused");
        resume(&d, "ta").await;
        for _ in 0..3 {
            pass(&mut d).await;
            trellis::staging::build::settle_builds_with(d.pool(), 3).await;
            d.settle().await;
        }
        assert_eq!(column_type(&d, "public.ta", "x").await, "bigint");
        assert_eq!(column_type(&d, "public.jn", "x").await, "bigint");
        let statuses = d
            .rows("select target_table, status::text from transform_definitions order by id")
            .await;
        assert_eq!(
            statuses,
            ["(public.ta,live)", "(public.jn,live)"],
            "{kind:?}"
        );
        chained_exact(&d, kind).await;
        step(&mut d, "update public.asrc set x = 3000000000 where id = 1").await;
        chained_exact(&d, kind).await;
    }
}

/// `jn` over `ta` against the join of `asrc` and `b`, and `self_check` clean.
async fn chained_exact(d: &Driver, kind: Kind) {
    assert_eq!(
        d.rows("select t.id::text, t.x::text, t.w::text from public.jn t order by t.id")
            .await,
        d.rows(&format!(
            "select a.id::text, a.x::text, b.w::text from public.asrc a \
             {} join public.b b on a.k = b.k order by a.id",
            kind.word()
        ))
        .await,
        "{kind:?}"
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), "public.jn")
        .await
        .expect("self_check");
    assert!(diverging.is_empty(), "{kind:?}: {diverging:?}");
}

// ------------------------------------------------ a chained 1-1 side as `b`

/// `tb` (a 1-1 target of `bsrc`) as `b`, joined on its primary key. A
/// widening of `bsrc.w` outgrows `tb.w`, so `tb` pauses; its `RESUME` re-types
/// `tb.w`, which outgrows the join's copies of it (`M.w` and the passthrough
/// `w`), read from `tb` alone: the join is paused with `tb`'s resume as its
/// cause and resumes on its own once `tb` is live. A field over both tables
/// (`a.r + tb.w`) may instead be found by the pass of `a`, whose pause records
/// no cause (#1539), so the join waits for its own `RESUME`. Either way it
/// rebuilds equal to the join, and a value past `integer` lands.
#[tokio::test]
async fn a_widening_under_a_chained_b_side_follows_its_upstream_resume() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        for (select, oracle, follows) in [
            ("tb.w AS w", "b.w", true),
            (
                "a.r + COALESCE(tb.w, 0) AS w",
                "a.r + coalesce(b.w, 0)",
                false,
            ),
        ] {
            set_enabled(true);
            let mut d = Driver::start(
                "create table public.a (id integer primary key, bid integer, r integer); \
                 create table public.bsrc (bid integer primary key, w integer); \
                 create index on public.a (bid); \
                 insert into public.a select i, i % 4, i from generate_series(1, 10) i; \
                 insert into public.bsrc select i, i * 100 from generate_series(1, 3) i",
                &[("w", int4())],
                &["TRANSFORM tb FROM public.bsrc SELECT w AS w"],
                &["public.a", "public.bsrc"],
            )
            .await;
            trellis::defs::install_definition(
                d.pool(),
                &format!(
                    "TRANSFORM jn FROM public.a {} JOIN public.tb ON a.bid = tb.bid \
                     SELECT {select}",
                    kind.word()
                ),
                &HashMap::from([("w".to_string(), int4())]),
                "public",
            )
            .await
            .expect("define the join over tb");
            pass(&mut d).await;
            trellis::intake::harness::settle_builds(d.pool()).await;
            d.settle().await;
            let actual = "select t.id::text, t.w::text from public.jn t order by t.id";
            let expected = format!(
                "select a.id::text, ({oracle})::text from public.a a {} join public.bsrc b \
                 on a.bid = b.bid order by a.id",
                kind.word()
            );
            assert_eq!(d.rows(actual).await, d.rows(&expected).await, "{kind:?}");

            write(&d, "alter table public.bsrc alter column w type bigint").await;
            pass(&mut d).await;
            assert_eq!(status(&d, "public.tb").await, "paused");
            assert_eq!(status(&d, "public.jn").await, "live");
            resume(&d, "tb").await;
            for _ in 0..3 {
                pass(&mut d).await;
                trellis::staging::build::settle_builds_with(d.pool(), 3).await;
                d.settle().await;
            }
            assert_eq!(status(&d, "public.tb").await, "live", "{kind:?} {select}");
            if follows {
                assert_eq!(status(&d, "public.jn").await, "live", "{kind:?} {select}");
            } else if status(&d, "public.jn").await == "paused" {
                let causes = d
                    .rows(
                        "select f.source_table, f.caused_by is null from capture_failures f \
                         join transform_definitions d on d.id = f.transform_id \
                         where d.target_table = 'public.jn'",
                    )
                    .await;
                assert_eq!(causes, ["(public.a,t)"], "{kind:?}");
                resume(&d, "jn").await;
                for _ in 0..3 {
                    pass(&mut d).await;
                    trellis::staging::build::settle_builds_with(d.pool(), 3).await;
                    d.settle().await;
                }
            }
            assert_eq!(status(&d, "public.jn").await, "live", "{kind:?} {select}");
            assert_eq!(column_type(&d, "public.jn", "w").await, "bigint");
            assert_eq!(hidden_type(&d, "public.jn", "m", "w").await, "bigint");
            step(
                &mut d,
                "update public.bsrc set w = 3000000000 where bid = 1",
            )
            .await;
            assert_eq!(
                d.rows(actual).await,
                d.rows(&expected).await,
                "{kind:?} {select}"
            );
            let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), "public.jn")
                .await
                .expect("self_check");
            assert!(diverging.is_empty(), "{kind:?}: {diverging:?}");
        }
    }
}

// ------------------------------- an aggregate's target as `a`, keyed by NULL

/// `agg` (counts per nullable `g`, so a group is keyed `NULL`) as `a`, joined
/// on its key to `b.k`. A widening of `src.g` pauses `agg`; its resume
/// re-types `agg.g`, which is the join's `a` key and `ON` column: the pair
/// no longer matches, so the join pauses naming `public.agg` and `g`, and its
/// `RESUME` refuses until `b.k` widens too. Then it rebuilds, the `NULL`-keyed
/// row among the rest.
#[tokio::test]
async fn a_widened_key_of_an_aggregate_a_side_keeps_its_null_keyed_rows() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        set_enabled(true);
        let mut d = Driver::start(
            "create table public.src (id integer primary key, g integer, v integer); \
             create table public.b (id integer primary key, k integer, w integer); \
             create unique index on public.b (k); \
             insert into public.src values \
                 (1, 1, 10), (2, null, 20), (3, 2, 5), (4, null, 7), (5, 3, 1); \
             insert into public.b values (1, 1, 100), (2, 2, 200), (3, null, 300)",
            &[("g", int4()), ("v", int4())],
            &[
                "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
               SELECT count(*) AS n",
            ],
            &["public.src"],
        )
        .await;
        trellis::defs::install_definition(
            d.pool(),
            &format!(
                "TRANSFORM jn FROM public.agg {} JOIN public.b ON agg.g = b.k \
                 SELECT agg.n + COALESCE(b.w, 0) AS total",
                kind.word()
            ),
            &HashMap::from([("total".to_string(), ValueType::Integer(IntWidth::Int8))]),
            "public",
        )
        .await
        .expect("define the join over agg");
        pass(&mut d).await;
        trellis::intake::harness::settle_builds(d.pool()).await;
        d.settle().await;
        let word = kind.word();
        let oracle = format!(
            "select agg.g::text, (agg.n + coalesce(b.w, 0))::text from public.agg agg \
             {word} join public.b b on agg.g = b.k order by 1"
        );
        let actual = "select g::text, total::text from public.jn order by 1";
        assert_eq!(d.rows(actual).await, d.rows(&oracle).await, "{kind:?}");

        write(&d, "alter table public.src alter column g type bigint").await;
        pass(&mut d).await;
        assert_eq!(status(&d, "public.agg").await, "paused");
        resume(&d, "agg").await;
        for _ in 0..3 {
            pass(&mut d).await;
            trellis::staging::build::settle_builds_with(d.pool(), 3).await;
            d.settle().await;
        }
        assert_eq!(status(&d, "public.agg").await, "live");
        assert_eq!(column_type(&d, "public.agg", "g").await, "bigint");
        let error = paused_for(&d, "public.jn", "public.agg", &["g"]).await;
        assert!(
            error.contains("don't match") && error.contains("A resume of public.agg re-typed"),
            "{kind:?}: {error}"
        );
        resume_refused(&d, "jn").await;

        write(&d, "alter table public.b alter column k type bigint").await;
        resume(&d, "jn").await;
        for _ in 0..3 {
            pass(&mut d).await;
            trellis::staging::build::settle_builds_with(d.pool(), 3).await;
            d.settle().await;
        }
        assert_eq!(status(&d, "public.jn").await, "live", "{kind:?}");
        assert_eq!(column_type(&d, "public.jn", "g").await, "bigint");
        assert_eq!(d.rows(actual).await, d.rows(&oracle).await, "{kind:?}");
        if kind == Kind::Left {
            let nulls = d.rows("select total from public.jn where g is null").await;
            assert_eq!(nulls.len(), 1, "the NULL-keyed group's row is kept");
        }
        let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), "public.jn")
            .await
            .expect("self_check");
        assert!(diverging.is_empty(), "{kind:?}: {diverging:?}");
        step(
            &mut d,
            "insert into public.src values (6, null, 1), (7, 3000000000, 1)",
        )
        .await;
        assert_eq!(d.rows(actual).await, d.rows(&oracle).await, "{kind:?}");
    }
}
