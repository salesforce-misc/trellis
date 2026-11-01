//! A group-basis aggregate (plain or `JOIN`) whose `GROUP BY` columns are
//! re-collated after define, between deterministic collations (#1558).
//!
//! The docs call such a change safe: byte equality is unchanged, so nothing
//! pauses and nothing is rebuilt. Changes keep applying and `self_check`
//! converges; the statements that meet the group columns of two tables of
//! different collations (the hidden table and its working tables, the source
//! and the hidden table) have to resolve a collation all the same. A rebuild
//! in place and a `RESUME` run them over every group.
//!
//! Each test steps the build by hand (`build::work_once`) to idle and asserts
//! where the definition ended: no test waits on convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::Config;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, Step, WorkerOptions};
use trellis::staging::self_check::{SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page};
use trellis::{Trellis, TrellisOptions};

fn options() -> WorkerOptions {
    WorkerOptions {
        chunk_rows: 3,
        drain_batch_cap: 10_000,
        heartbeat_interval: std::time::Duration::from_secs(1),
        reclaim_ttl: std::time::Duration::from_secs(60),
    }
}

/// Runs build steps until one is idle: a failed job is not claimable again
/// until its backoff passes, so a rebuild that fails ends the loop too.
async fn work_to_idle(d: &Driver) {
    for _ in 0..200 {
        let step = build::work_once(d.pool(), "builder", &options())
            .await
            .expect("a build step");
        if matches!(step, Step::Idle) {
            return;
        }
    }
    panic!("the build never went idle");
}

async fn trellis(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

async fn status_and_error(d: &Driver, target: &str) -> (String, Option<String>) {
    let status: String = d
        .ctl
        .query_one(
            "select status from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("the definition")
        .get(0);
    let error: Option<String> = d
        .ctl
        .query_one(
            "select string_agg(last_error, '; ') from backfill_chunks c \
             join transform_definitions t on t.id = c.definition_id \
             where t.target_table = $1 and c.last_error is not null",
            &[&target],
        )
        .await
        .expect("the build's jobs")
        .get(0);
    (status, error)
}

/// A walk of `target`'s check, a page of one group and of every group, finds
/// no divergence.
async fn assert_converged(d: &Driver, target: &str, case: &str) {
    for limit in [1, 1000] {
        let mut after = None;
        loop {
            let page = check_page(
                d.pool(),
                target,
                SelfCheckScope { after, limit },
                SelfCheckMode::Standard,
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|e| panic!("{case}: a page of {target}'s check: {e}"));
            assert_eq!(
                page.outcome,
                SelfCheckOutcome::Converged,
                "{case}: {target} at {limit}"
            );
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
    }
}

/// Source `a` (group columns `g1` and `g2`; `k` and `kt` join it to `b`) and
/// `b` (`h` is its group column), with groups that order otherwise under
/// `und-x-icu` than by bytes (`B` sorts after `a` under ICU, before it by
/// bytes). Each column carries the collation named for it.
fn ddl(a_g1: &str, a_g2: &str, kt: &str, b_h: &str) -> String {
    format!(
        "create table public.a (id integer primary key, g1 text collate \"{a_g1}\", \
             g2 text collate \"{a_g2}\", k integer, kt text collate \"{kt}\", v integer); \
         create table public.b (id integer primary key, k integer, kt text collate \"{kt}\", \
             y integer, h text collate \"{b_h}\"); \
         create index on public.a (k); create index on public.b (k); \
         create index on public.a (kt); create index on public.b (kt); \
         insert into public.a \
             select i, (array['a', 'B', 'c', 'D'])[1 + i % 4], (array['x', 'Y', 'z'])[1 + i % 3], \
                    i % 5, 'k' || i % 5, i \
             from generate_series(1, 16) i; \
         insert into public.b \
             select i, i % 4, 'k' || i % 4, i * 10, (array['p', 'Q', 'r'])[1 + i % 3] \
             from generate_series(1, 7) i"
    )
}

const COLUMNS: &[(&str, ValueType)] = &[
    ("id", ValueType::Integer(IntWidth::Int4)),
    ("g1", ValueType::Text),
    ("g2", ValueType::Text),
    ("k", ValueType::Integer(IntWidth::Int4)),
    ("kt", ValueType::Text),
    ("v", ValueType::Integer(IntWidth::Int4)),
    ("y", ValueType::Integer(IntWidth::Int4)),
    ("h", ValueType::Text),
];

/// One re-collation case: the source tables as created, the definition, the
/// `GROUP BY` columns, what an oracle computes from the source tables, and
/// the `alter table ...` statements run once the definition is built.
struct Case<'a> {
    name: &'a str,
    ddl: String,
    definition: &'a str,
    /// The tables capture is installed on.
    captured: &'a [&'a str],
    /// The target's group columns, `g1, g2`.
    groups: &'a str,
    /// The oracle query, its select list the target's columns and the groups
    /// grouped, without an ordering.
    oracle: &'a str,
    /// The target's value columns after the groups, `n, sy`.
    values: &'a str,
    recollate: &'a [&'a str],
}

const BOTH: &[&str] = &["public.a", "public.b"];

impl Case<'_> {
    fn order(&self) -> String {
        self.groups
            .split(", ")
            .map(|g| format!("{g} collate \"C\""))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The target holds what the oracle computes from the sources, and a
    /// check of it finds no divergence.
    async fn assert_matches(&self, d: &Driver, stage: &str) {
        let order = self.order();
        let target = d
            .rows(&format!(
                "select {}, {} from public.t order by {order}",
                self.groups, self.values
            ))
            .await;
        let oracle = d.rows(&format!("{} order by {order}", self.oracle)).await;
        assert_eq!(target, oracle, "{}: {stage}", self.name);
        assert_converged(d, "public.t", self.name).await;
    }
}

/// The definition is built; the source is re-collated. Changes apply
/// (including groups the target has not seen, and image-less ones that
/// recompute their groups), the check finds no divergence, a rebuild in
/// place finishes, and a `RESUME` after changes while paused finishes.
async fn a_re_collated_group_applies_checks_rebuilds_and_resumes(c: Case<'_>) {
    let name = c.name;
    let mut d = Driver::start(&c.ddl, COLUMNS, &[c.definition], c.captured).await;
    c.assert_matches(&d, "built").await;
    for stmt in c.recollate {
        d.ctl
            .batch_execute(stmt)
            .await
            .unwrap_or_else(|e| panic!("{name}: {stmt}: {e}"));
    }
    d.user()
        .await
        .batch_execute(
            "update public.a set v = v + 100 where id % 3 = 0; \
             delete from public.a where id in (4, 9); \
             insert into public.a select id + 100, g1 || 'n', g2, k, kt, v from public.a \
                 where id < 6; \
             insert into public.b values (20, 2, 'k2', 7, 'zz'); \
             update public.b set y = y + 1 where id < 3",
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: changes to apply: {e}"));
    d.settle().await;
    c.assert_matches(&d, "applied").await;

    // A change with no image names its group, and the page recomputes it
    // from the source's rows.
    d.ctl
        .batch_execute(
            "update public.a set v = v + 1000 where id = 2; \
             update public.b set y = y + 1000 where id = 1",
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: change under the capture: {e}"));
    d.stage_recomputes("public.a", &["1", "2", "5"]).await;
    if c.captured.contains(&"public.b") {
        d.stage_recomputes("public.b", &["1", "2"]).await;
    }
    d.settle().await;
    c.assert_matches(&d, "recomputed").await;

    let trellis = trellis(&d).await;
    rebuild_in_place(&mut d, &trellis, &c, "rebuilt").await;
    pause_and_resume(&mut d, &trellis, &c, "resumed").await;

    // Changes apply on the resumed hidden tables, and a second rebuild
    // finishes.
    d.user()
        .await
        .batch_execute(
            "update public.a set v = v + 5 where id % 2 = 0; \
             insert into public.a values (201, 'yy', 'p', 2, 'k2', 9)",
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: changes after the resume: {e}"));
    d.settle().await;
    c.assert_matches(&d, "applied after the resume").await;
    rebuild_in_place(&mut d, &trellis, &c, "rebuilt again").await;
}

/// A rebuild in place finishes, and the target then matches.
async fn rebuild_in_place(d: &mut Driver, trellis: &Trellis, c: &Case<'_>, stage: &str) {
    let name = c.name;
    trellis
        .request_backfill("public.a")
        .await
        .unwrap_or_else(|e| panic!("{name}: {stage}: request a rebuild: {e}"));
    work_to_idle(d).await;
    let (status, error) = status_and_error(d, "public.t").await;
    assert_eq!(
        (status.as_str(), error),
        ("live", None),
        "{name}: {stage}: the rebuild finishes"
    );
    d.settle().await;
    c.assert_matches(d, stage).await;
}

/// A `PAUSE`, changes while paused, and a `RESUME` that finishes, which
/// re-creates the hidden tables at the sources' collations and keeps the
/// target's; the target then matches.
async fn pause_and_resume(d: &mut Driver, trellis: &Trellis, c: &Case<'_>, stage: &str) {
    let name = c.name;
    trellis
        .apply("PAUSE TRANSFORM t")
        .await
        .unwrap_or_else(|e| panic!("{name}: {stage}: pause: {e}"));
    d.user()
        .await
        .batch_execute(
            "delete from public.a where id > 100; update public.a set v = v + 1; \
             insert into public.a values (200, 'zzz', 'q', 1, 'k1', 5)",
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: {stage}: changes while paused: {e}"));
    d.settle().await;
    trellis
        .apply("RESUME TRANSFORM t")
        .await
        .unwrap_or_else(|e| panic!("{name}: {stage}: resume: {e}"));
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .unwrap_or_else(|e| panic!("{name}: {stage}: start the resumed build: {e}"));
    work_to_idle(d).await;
    let (status, error) = status_and_error(d, "public.t").await;
    assert_eq!(
        (status.as_str(), error),
        ("live", None),
        "{name}: {stage}: the resume finishes"
    );
    d.settle().await;
    c.assert_matches(d, stage).await;
}

fn alter(table: &str, column: &str, collation: &str) -> String {
    format!("alter table public.{table} alter column {column} type text collate \"{collation}\"")
}

// ---------------------------------------------------------------- JOIN

const JOIN_ONE: &str = "TRANSFORM t FROM public.a JOIN public.b ON a.k = b.k GROUP BY a.g1 \
                        WITHOUT SOURCE INDEX \
                        SELECT a.g1 AS g1, COUNT(*) AS n, SUM(b.y) AS sy";
const JOIN_ONE_ORACLE: &str = "select a.g1, count(*), sum(b.y) from public.a \
                               join public.b on a.k = b.k group by a.g1";

const JOIN_TWO: &str = "TRANSFORM t FROM public.a JOIN public.b ON a.k = b.k \
                        GROUP BY a.g1, a.g2 WITHOUT SOURCE INDEX \
                        SELECT a.g1 AS g1, a.g2 AS g2, COUNT(*) AS n, SUM(b.y) AS sy";
const JOIN_TWO_ORACLE: &str = "select a.g1, a.g2, count(*), sum(b.y) from public.a \
                               join public.b on a.k = b.k group by a.g1, a.g2";

/// The issue's repro: a `JOIN` aggregate's one group column, ICU then POSIX.
#[tokio::test]
async fn a_join_group_column_re_collated() {
    a_re_collated_group_applies_checks_rebuilds_and_resumes(Case {
        name: "join, one column, icu then POSIX",
        ddl: ddl("und-x-icu", "default", "default", "default"),
        definition: JOIN_ONE,
        captured: BOTH,
        groups: "g1",
        oracle: JOIN_ONE_ORACLE,
        values: "n, sy",
        recollate: &[&alter("a", "g1", "POSIX")],
    })
    .await;
}

/// The other directions: from the database's default, to it, and from a
/// byte-order collation to ICU.
#[tokio::test]
async fn a_join_group_column_re_collated_from_or_to_the_default() {
    for (name, from, to) in [
        ("POSIX then icu", "POSIX", "und-x-icu"),
        ("default then icu", "default", "und-x-icu"),
        ("icu then default", "und-x-icu", "default"),
        ("C then POSIX", "C", "POSIX"),
    ] {
        a_re_collated_group_applies_checks_rebuilds_and_resumes(Case {
            name,
            ddl: ddl(from, "default", "default", "default"),
            definition: JOIN_ONE,
            captured: BOTH,
            groups: "g1",
            oracle: JOIN_ONE_ORACLE,
            values: "n, sy",
            recollate: &[&alter("a", "g1", to)],
        })
        .await;
    }
}

/// Two group columns, one or both re-collated.
#[tokio::test]
async fn a_join_with_two_group_columns_re_collated() {
    for (name, from, to) in [
        (
            "icu twice, then icu and POSIX",
            ("und-x-icu", "und-x-icu"),
            vec![alter("a", "g2", "POSIX")],
        ),
        (
            "icu and POSIX, then icu twice",
            ("und-x-icu", "POSIX"),
            vec![alter("a", "g2", "und-x-icu")],
        ),
        (
            "icu twice, then POSIX twice",
            ("und-x-icu", "und-x-icu"),
            vec![alter("a", "g1", "POSIX"), alter("a", "g2", "POSIX")],
        ),
        (
            "default and icu, then icu and default",
            ("default", "und-x-icu"),
            vec![alter("a", "g1", "und-x-icu"), alter("a", "g2", "default")],
        ),
    ] {
        a_re_collated_group_applies_checks_rebuilds_and_resumes(Case {
            name,
            ddl: ddl(from.0, from.1, "default", "default"),
            definition: JOIN_TWO,
            captured: BOTH,
            groups: "g1, g2",
            oracle: JOIN_TWO_ORACLE,
            values: "n, sy",
            recollate: &to.iter().map(String::as_str).collect::<Vec<_>>(),
        })
        .await;
    }
}

/// A `LEFT JOIN`, whose unmatched groups come from the `FROM` side alone.
#[tokio::test]
async fn a_left_join_group_column_re_collated() {
    a_re_collated_group_applies_checks_rebuilds_and_resumes(Case {
        name: "left join, icu then POSIX",
        ddl: ddl("und-x-icu", "default", "default", "default"),
        definition: "TRANSFORM t FROM public.a LEFT JOIN public.b ON a.k = b.k GROUP BY a.g1 \
                     WITHOUT SOURCE INDEX \
                     SELECT a.g1 AS g1, COUNT(*) AS n, SUM(b.y) AS sy",
        captured: BOTH,
        groups: "g1",
        oracle: "select a.g1, count(*), sum(b.y) from public.a \
                 left join public.b on a.k = b.k group by a.g1",
        values: "n, sy",
        recollate: &[&alter("a", "g1", "POSIX")],
    })
    .await;
}

/// The joined table's group column re-collated, beside the `FROM` table's.
#[tokio::test]
async fn a_join_group_column_of_the_joined_table_re_collated() {
    a_re_collated_group_applies_checks_rebuilds_and_resumes(Case {
        name: "joined table's column, icu then POSIX",
        ddl: ddl("und-x-icu", "default", "default", "und-x-icu"),
        definition: "TRANSFORM t FROM public.a JOIN public.b ON a.k = b.k GROUP BY a.g1, b.h \
                     WITHOUT SOURCE INDEX \
                     SELECT a.g1 AS g1, b.h AS h, COUNT(*) AS n, SUM(b.y) AS sy",
        captured: BOTH,
        groups: "g1, h",
        oracle: "select a.g1, b.h, count(*), sum(b.y) from public.a \
                 join public.b on a.k = b.k group by a.g1, b.h",
        values: "n, sy",
        recollate: &[&alter("b", "h", "POSIX")],
    })
    .await;
}

/// A text `ON` column of both tables re-collated, which pairs them still.
#[tokio::test]
async fn a_join_on_column_re_collated_on_both_sides() {
    a_re_collated_group_applies_checks_rebuilds_and_resumes(Case {
        name: "text ON column, icu then POSIX",
        ddl: ddl("und-x-icu", "default", "und-x-icu", "default"),
        definition: "TRANSFORM t FROM public.a JOIN public.b ON a.kt = b.kt GROUP BY a.g1 \
                     WITHOUT SOURCE INDEX \
                     SELECT a.g1 AS g1, COUNT(*) AS n, SUM(b.y) AS sy",
        captured: BOTH,
        groups: "g1",
        oracle: "select a.g1, count(*), sum(b.y) from public.a \
                 join public.b on a.kt = b.kt group by a.g1",
        values: "n, sy",
        recollate: &[
            &alter("a", "kt", "POSIX"),
            &alter("b", "kt", "POSIX"),
            &alter("a", "g1", "POSIX"),
        ],
    })
    .await;
}

/// The source, the hidden tables and the target at three collations at once:
/// re-collated, resumed (the hidden tables follow the sources), re-collated
/// again. Changes, image-less recomputes, a rebuild, and a `TRUNCATE` of each
/// side (the joined side's refills a `LEFT JOIN`'s unmatched groups) all
/// meet the three.
#[tokio::test]
async fn a_join_whose_source_hidden_tables_and_target_have_three_collations() {
    for (c, second) in [
        (
            Case {
                name: "three collations, two tables' groups",
                ddl: ddl("und-x-icu", "default", "default", "und-x-icu"),
                definition: "TRANSFORM t FROM public.a JOIN public.b ON a.k = b.k \
                             GROUP BY a.g1, b.h WITHOUT SOURCE INDEX \
                             SELECT a.g1 AS g1, b.h AS h, COUNT(*) AS n, SUM(b.y) AS sy",
                captured: BOTH,
                groups: "g1, h",
                oracle: "select a.g1, b.h, count(*), sum(b.y) from public.a \
                         join public.b on a.k = b.k group by a.g1, b.h",
                values: "n, sy",
                recollate: &[&alter("a", "g1", "POSIX"), &alter("b", "h", "C")],
            },
            [alter("a", "g1", "C"), alter("b", "h", "default")],
        ),
        (
            Case {
                name: "three collations, LEFT on the grouped ON column",
                ddl: ddl("default", "default", "und-x-icu", "default"),
                definition: "TRANSFORM t FROM public.a LEFT JOIN public.b ON a.kt = b.kt \
                             GROUP BY a.kt WITHOUT SOURCE INDEX \
                             SELECT a.kt AS kt, COUNT(*) AS n, SUM(b.y) AS sy",
                captured: BOTH,
                groups: "kt",
                oracle: "select * from (select a.kt as kt, count(*), sum(b.y) from public.a \
                         left join public.b on a.kt = b.kt group by a.kt) as q",
                values: "n, sy",
                recollate: &[&alter("a", "kt", "POSIX"), &alter("b", "kt", "POSIX")],
            },
            [alter("a", "kt", "C"), alter("b", "kt", "C")],
        ),
    ] {
        let name = c.name;
        let mut d = Driver::start(&c.ddl, COLUMNS, &[c.definition], c.captured).await;
        let trellis = trellis(&d).await;
        c.assert_matches(&d, "built").await;
        for stmt in c.recollate {
            d.ctl
                .batch_execute(stmt)
                .await
                .unwrap_or_else(|e| panic!("{name}: {stmt}: {e}"));
        }
        pause_and_resume(&mut d, &trellis, &c, "resumed").await;
        for stmt in &second {
            d.ctl
                .batch_execute(stmt)
                .await
                .unwrap_or_else(|e| panic!("{name}: {stmt}: {e}"));
        }
        d.user()
            .await
            .batch_execute(
                "update public.a set v = v + 7 where id % 3 = 1; \
                 insert into public.a select id + 300, g1 || 'n', g2, k, kt, v from public.a \
                     where id < 4; \
                 insert into public.b select id + 300, k, kt, y, h || 'n' from public.b \
                     where id < 3",
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: changes: {e}"));
        d.settle().await;
        c.assert_matches(&d, "applied").await;
        d.ctl
            .batch_execute(
                "update public.a set v = v + 1000 where id = 2; \
                 update public.b set y = y + 1000 where id = 1",
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: change under the capture: {e}"));
        d.stage_recomputes("public.a", &["1", "2", "5"]).await;
        d.stage_recomputes("public.b", &["1", "2"]).await;
        d.settle().await;
        c.assert_matches(&d, "recomputed").await;
        rebuild_in_place(&mut d, &trellis, &c, "rebuilt").await;
        for truncate in [
            "truncate public.b; \
             insert into public.b values (1, 1, 'k1', 10, 'p'), (2, 2, 'k2', 20, 'Q')",
            "truncate public.a; \
             insert into public.a values (1, 'a', 'x', 1, 'k1', 1), (2, 'B', 'Y', 2, 'k2', 2), \
                 (3, 'B', 'Y', 9, 'k9', 3)",
        ] {
            d.user()
                .await
                .batch_execute(truncate)
                .await
                .unwrap_or_else(|e| panic!("{name}: {truncate}: {e}"));
            d.settle().await;
            c.assert_matches(&d, truncate).await;
        }
        rebuild_in_place(&mut d, &trellis, &c, "rebuilt after the truncates").await;
    }
}

// ------------------------------------------------------------ plain aggregate

const PLAIN_ONE: &str = "TRANSFORM t FROM public.a GROUP BY g1 WITHOUT SOURCE INDEX \
                         SELECT COUNT(*) AS n, SUM(v) AS sy";
const PLAIN_ONE_ORACLE: &str = "select g1, count(*), sum(v) from public.a group by g1";

const PLAIN_TWO: &str = "TRANSFORM t FROM public.a GROUP BY g1, g2 WITHOUT SOURCE INDEX \
                         SELECT COUNT(*) AS n, SUM(v) AS sy";
const PLAIN_TWO_ORACLE: &str = "select g1, g2, count(*), sum(v) from public.a group by g1, g2";

/// The control: a plain aggregate's group columns are the database's default
/// collation whatever the source's, and pass through every step.
#[tokio::test]
async fn a_plain_aggregate_group_column_re_collated() {
    a_re_collated_group_applies_checks_rebuilds_and_resumes(Case {
        name: "plain, one column, icu then POSIX",
        ddl: ddl("und-x-icu", "default", "default", "default"),
        definition: PLAIN_ONE,
        captured: &["public.a"],
        groups: "g1",
        oracle: PLAIN_ONE_ORACLE,
        values: "n, sy",
        recollate: &[&alter("a", "g1", "POSIX")],
    })
    .await;
    a_re_collated_group_applies_checks_rebuilds_and_resumes(Case {
        name: "plain, two columns, icu twice then icu and POSIX",
        ddl: ddl("und-x-icu", "und-x-icu", "default", "default"),
        definition: PLAIN_TWO,
        captured: &["public.a"],
        groups: "g1, g2",
        oracle: PLAIN_TWO_ORACLE,
        values: "n, sy",
        recollate: &[&alter("a", "g2", "POSIX")],
    })
    .await;
}

// ------------------------------------------------------- partition correction

/// A capture pass over every table a definition reads.
async fn pass(d: &mut Driver) -> trellis::capture::reconcile::PassOutcome {
    let desired = trellis::defs::tables_to_capture(d.pool())
        .await
        .expect("tables to capture");
    trellis::capture::reconcile::reconcile(
        &mut d.ctl,
        trellis::config::DEFAULT_SCHEMA,
        &desired,
        std::time::Instant::now() + std::time::Duration::from_secs(5),
    )
    .await
    .expect("a capture pass")
}

/// A leaf of a partitioned `FROM` table dropped with changes undrained, after
/// the group column was re-collated: the correction names the cells to
/// recompute from the hidden table's cells and the ring's images, which carry
/// the source's collation, in one deduplicated union.
#[tokio::test]
async fn a_dropped_leaf_of_a_re_collated_join_is_corrected() {
    let name = "dropped leaf, icu then POSIX";
    let mut d = Driver::start(
        "create table public.b (id integer primary key, k integer, y integer); \
         create index on public.b (k); \
         create table public.a (id integer not null, g1 text collate \"und-x-icu\", \
             k integer not null, x integer, primary key (k, id)) partition by range (k); \
         create table public.a_p0 partition of public.a for values from (0) to (4); \
         create table public.a_p1 partition of public.a for values from (4) to (8); \
         create index on public.a (k)",
        COLUMNS,
        &[
            "TRANSFORM t FROM public.a JOIN public.b ON a.k = b.k GROUP BY a.g1 \
             WITHOUT SOURCE INDEX SELECT a.g1 AS g1, COUNT(*) AS n, SUM(a.x) AS sx, \
             SUM(b.y) AS sy",
        ],
        &["public.a", "public.b"],
    )
    .await;
    // The first sight of the source records its tree; the seed arrives
    // through capture, as a partitioned side with rows has no build.
    assert!(pass(&mut d).await.failed.is_empty());
    d.ctl
        .batch_execute(
            "insert into public.b select i, i % 8, i * 10 from generate_series(1, 16) i; \
             insert into public.a \
                 select i, (array['a', 'B', 'c', 'D'])[1 + i % 4], i % 8, i \
                 from generate_series(1, 40) i",
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: seed: {e}"));
    d.settle().await;
    d.ctl
        .batch_execute(&alter("a", "g1", "POSIX"))
        .await
        .unwrap_or_else(|e| panic!("{name}: re-collate: {e}"));
    // Changes the ring still holds, naming a cell the hidden table does not,
    // in the leaf that is then dropped.
    d.ctl
        .batch_execute(
            "insert into public.a select 500 + i, 'new' || i, 5, i from generate_series(1, 3) i; \
             update public.a_p1 set x = x + 100 where id % 3 = 0; \
             drop table public.a_p1",
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: changes and the drop: {e}"));
    assert!(pass(&mut d).await.failed.is_empty());
    let outcome = trellis::staging::partition_correction::run_pending(
        d.pool(),
        &trellis::staging::partition_correction::Options::default(),
    )
    .await
    .unwrap_or_else(|e| panic!("{name}: run the correction: {e}"));
    assert_eq!(outcome.completed, 1, "{name}: the correction completes");
    d.settle().await;
    let case = Case {
        name,
        ddl: String::new(),
        definition: "",
        captured: BOTH,
        groups: "g1",
        oracle: "select a.g1, count(*), sum(a.x), sum(b.y) from public.a \
                 join public.b on a.k = b.k group by a.g1",
        values: "n, sx, sy",
        recollate: &[],
    };
    case.assert_matches(&d, "corrected").await;
}
