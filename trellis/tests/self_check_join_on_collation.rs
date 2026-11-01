//! A `JOIN` whose text `ON` column is re-collated after define on one side
//! only, between deterministic collations (#1564).
//!
//! The docs call such a change safe: byte equality is unchanged, even across
//! a join pair, so nothing pauses. Changes keep applying, but `self_check`
//! runs the definition's own `JOIN ... ON` over the two source tables, and a
//! pair of two different non-default collations has no collation Postgres
//! can choose: every page of the target's check failed with `could not
//! determine which collation to use`. The 1-1 `JOIN`'s check meets the same
//! pair, and so does a page that finds a change of the re-collated side not
//! applied yet in the hidden table it lands in, whose copy of the column
//! keeps the collation define gave it. (A rebuild or a change of the `FROM`
//! side of a 1-1 `JOIN` meets it elsewhere, #1558 and #1567.)
//!
//! No test waits on convergence: changes are applied by sealing and draining
//! until nothing is pending, and a page that reads changes not applied yet
//! is compared without the wait (`compare_page`).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::integer::IntWidth;
use trellis::staging::self_check::{
    SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};

/// The 1-1 `JOIN` test turns the dev knob on, which is process-wide.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A walk of `target`'s check, a page of one key and of every key, finds no
/// divergence.
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

/// One comparison of every page of `target`'s check (`target_part_only`:
/// of a `JOIN` target's target part, `t`), a page of one key and of every
/// key, with no convergence wait, whatever the ring holds: it finds no
/// divergence and can vouch for every key.
async fn assert_compares_clean(d: &Driver, target: &str, target_part_only: bool, case: &str) {
    for limit in [1, 1000] {
        let mut after: Option<String> = None;
        loop {
            let page = compare_page(d.pool(), target, SelfCheckScope { after, limit })
                .await
                .unwrap_or_else(|e| panic!("{case}: a comparison of {target}: {e}"));
            assert_eq!(
                (page.divergences, page.uncertain_keys, page.uncertain_all),
                (vec![], vec![], false),
                "{case}: {target} at {limit}"
            );
            after = page.next_after;
            let past = |after: &str| target_part_only && !after.starts_with("t:");
            if after.as_deref().is_none_or(past) {
                break;
            }
        }
    }
}

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

const COLUMNS: &[(&str, ValueType)] = &[
    ("id", ValueType::Integer(IntWidth::Int4)),
    ("g1", ValueType::Text),
    ("kt", ValueType::Text),
    ("kt2", ValueType::Text),
    ("k", ValueType::Integer(IntWidth::Int4)),
    ("v", ValueType::Integer(IntWidth::Int4)),
    ("y", ValueType::Integer(IntWidth::Int4)),
];

/// Source `a` and `b`, joined on `kt` (and, for the composite cases, `kt2` or
/// `k`), each text `ON` column of the collation named for it.
fn ddl(a_kt: &str, b_kt: &str, a_kt2: &str, b_kt2: &str) -> String {
    format!(
        "create table public.a (id integer primary key, g1 text, \
             kt text collate \"{a_kt}\", kt2 text collate \"{a_kt2}\", k integer, v integer); \
         create table public.b (id integer primary key, kt text collate \"{b_kt}\", \
             kt2 text collate \"{b_kt2}\", k integer, y integer); \
         create index on public.a (kt, kt2, k); create index on public.b (kt, kt2, k); \
         insert into public.a \
             select i, (array['a', 'B', 'c', 'D'])[1 + i % 4], 'k' || i % 5, 'm' || i % 3, \
                    i % 2, i \
             from generate_series(1, 16) i; \
         insert into public.b \
             select i, 'k' || i % 4, 'm' || i % 3, i % 2, i * 10 \
             from generate_series(1, 7) i"
    )
}

struct Case<'a> {
    name: &'a str,
    ddl: String,
    definition: &'a str,
    /// The oracle query over the source tables, comparing text `ON` pairs
    /// by their bytes, its select list the target's columns, `g1, n, sy`.
    oracle: &'a str,
    recollate: &'a [&'a str],
}

impl Case<'_> {
    async fn assert_matches(&self, d: &Driver, stage: &str) {
        let target = d
            .rows("select g1, n, sy from public.t order by g1 collate \"C\"")
            .await;
        let oracle = d
            .rows(&format!(
                "select * from ({}) q(g1, n, sy) order by g1 collate \"C\"",
                self.oracle
            ))
            .await;
        assert_eq!(target, oracle, "{}: {stage}", self.name);
        assert_converged(d, "public.t", self.name).await;
    }
}

fn alter(table: &str, column: &str, collation: &str) -> String {
    format!("alter table public.{table} alter column {column} type text collate \"{collation}\"")
}

/// The definition is built and checked; the source is re-collated. Changes
/// apply and the check finds no divergence.
async fn a_re_collated_on_column_is_checked_without_a_divergence(c: Case<'_>) {
    let name = c.name;
    let mut d = Driver::start(&c.ddl, COLUMNS, &[c.definition], &["public.a", "public.b"]).await;
    c.assert_matches(&d, "built").await;
    for stmt in c.recollate {
        d.ctl
            .batch_execute(stmt)
            .await
            .unwrap_or_else(|e| panic!("{name}: {stmt}: {e}"));
    }
    c.assert_matches(&d, "re-collated").await;
    d.user()
        .await
        .batch_execute(
            "update public.a set v = v + 100 where id % 3 = 0; \
             delete from public.a where id in (4, 9); \
             insert into public.a select id + 100, g1 || 'n', kt, kt2, k, v from public.a \
                 where id < 6; \
             insert into public.b values (20, 'k2', 'm2', 0, 7), (21, 'k9', 'm0', 1, 9); \
             update public.b set y = y + 1 where id < 3; \
             delete from public.b where id = 5",
        )
        .await
        .unwrap_or_else(|e| panic!("{name}: changes to apply: {e}"));
    d.settle().await;
    c.assert_matches(&d, "applied").await;
}

const INNER: &str = "TRANSFORM t FROM public.a JOIN public.b ON a.kt = b.kt GROUP BY a.g1 \
                     WITHOUT SOURCE INDEX \
                     SELECT a.g1 AS g1, COUNT(*) AS n, SUM(b.y) AS sy";
const INNER_ORACLE: &str = "select a.g1, count(*), sum(b.y) from public.a \
                            join public.b on a.kt = b.kt collate \"C\" group by a.g1";

const LEFT: &str = "TRANSFORM t FROM public.a LEFT JOIN public.b ON a.kt = b.kt GROUP BY a.g1 \
                    WITHOUT SOURCE INDEX \
                    SELECT a.g1 AS g1, COUNT(*) AS n, SUM(b.y) AS sy";
const LEFT_ORACLE: &str = "select a.g1, count(*), sum(b.y) from public.a \
                           left join public.b on a.kt = b.kt collate \"C\" group by a.g1";

/// The issue's repro: `a.kt` ICU to POSIX, `b.kt` stays ICU.
#[tokio::test]
async fn a_join_on_column_re_collated_on_the_from_side() {
    a_re_collated_on_column_is_checked_without_a_divergence(Case {
        name: "inner, a.kt icu then POSIX",
        ddl: ddl("und-x-icu", "und-x-icu", "default", "default"),
        definition: INNER,
        oracle: INNER_ORACLE,
        recollate: &[&alter("a", "kt", "POSIX")],
    })
    .await;
}

/// The joined side re-collated, and a `LEFT JOIN`'s match (`__bn`) too.
#[tokio::test]
async fn a_join_on_column_re_collated_on_the_joined_side() {
    for (name, definition, oracle) in [
        ("inner, b.kt icu then POSIX", INNER, INNER_ORACLE),
        ("left, b.kt icu then POSIX", LEFT, LEFT_ORACLE),
    ] {
        a_re_collated_on_column_is_checked_without_a_divergence(Case {
            name,
            ddl: ddl("und-x-icu", "und-x-icu", "default", "default"),
            definition,
            oracle,
            recollate: &[&alter("b", "kt", "POSIX")],
        })
        .await;
    }
}

/// A `LEFT JOIN` whose `FROM` side is re-collated, from and to the
/// database's default and a byte-order collation.
#[tokio::test]
async fn a_left_join_on_column_re_collated_from_or_to_the_default() {
    for (name, define, table, to) in [
        ("left, a.kt icu then POSIX", "und-x-icu", "a", "POSIX"),
        ("left, a.kt default then icu", "default", "a", "und-x-icu"),
        ("left, b.kt icu then default", "und-x-icu", "b", "default"),
        ("left, a.kt C then POSIX", "C", "a", "POSIX"),
    ] {
        a_re_collated_on_column_is_checked_without_a_divergence(Case {
            name,
            ddl: ddl(define, define, "default", "default"),
            definition: LEFT,
            oracle: LEFT_ORACLE,
            recollate: &[&alter(table, "kt", to)],
        })
        .await;
    }
}

/// A composite `ON`: a text pair re-collated, beside a text pair that is not
/// and an integer pair; and two text pairs re-collated on different sides.
#[tokio::test]
async fn a_composite_join_with_a_text_pair_re_collated() {
    a_re_collated_on_column_is_checked_without_a_divergence(Case {
        name: "composite, a.kt icu then POSIX",
        ddl: ddl("und-x-icu", "und-x-icu", "und-x-icu", "und-x-icu"),
        definition: "TRANSFORM t FROM public.a JOIN public.b \
                     ON a.kt = b.kt AND a.kt2 = b.kt2 AND a.k = b.k GROUP BY a.g1 \
                     WITHOUT SOURCE INDEX \
                     SELECT a.g1 AS g1, COUNT(*) AS n, SUM(b.y) AS sy",
        oracle: "select a.g1, count(*), sum(b.y) from public.a join public.b \
                 on a.kt = b.kt collate \"C\" and a.kt2 = b.kt2 and a.k = b.k group by a.g1",
        recollate: &[&alter("a", "kt", "POSIX")],
    })
    .await;
    a_re_collated_on_column_is_checked_without_a_divergence(Case {
        name: "composite left, a.kt and b.kt2 icu then POSIX",
        ddl: ddl("und-x-icu", "und-x-icu", "und-x-icu", "und-x-icu"),
        definition: "TRANSFORM t FROM public.a LEFT JOIN public.b \
                     ON a.kt = b.kt AND a.kt2 = b.kt2 GROUP BY a.g1 \
                     WITHOUT SOURCE INDEX \
                     SELECT a.g1 AS g1, COUNT(*) AS n, SUM(b.y) AS sy",
        oracle: "select a.g1, count(*), sum(b.y) from public.a left join public.b \
                 on a.kt = b.kt collate \"C\" and a.kt2 = b.kt2 collate \"C\" group by a.g1",
        recollate: &[&alter("a", "kt", "POSIX"), &alter("b", "kt2", "POSIX")],
    })
    .await;
}

/// The `ON` written with the joined table first.
#[tokio::test]
async fn a_join_on_written_joined_table_first_with_a_re_collated_column() {
    a_re_collated_on_column_is_checked_without_a_divergence(Case {
        name: "inner, b.kt = a.kt, a.kt icu then POSIX",
        ddl: ddl("und-x-icu", "und-x-icu", "default", "default"),
        definition: "TRANSFORM t FROM public.a JOIN public.b ON b.kt = a.kt GROUP BY a.g1 \
                     WITHOUT SOURCE INDEX \
                     SELECT a.g1 AS g1, COUNT(*) AS n, SUM(b.y) AS sy",
        oracle: INNER_ORACLE,
        recollate: &[&alter("a", "kt", "POSIX")],
    })
    .await;
}

/// Changes to either side not applied yet, after that side's `ON` column (or
/// a `GROUP BY` column) was re-collated: a page subtracts each from the cell
/// of `P` or `M` it lands in, found by the column's copy there, which keeps
/// the collation define gave it. Applying them changes nothing either.
#[tokio::test]
async fn changes_not_applied_yet_after_a_re_collation_compare_clean() {
    for (name, definition, oracle, table, column) in [
        ("inner, a.kt icu then POSIX", INNER, INNER_ORACLE, "a", "kt"),
        ("inner, b.kt icu then POSIX", INNER, INNER_ORACLE, "b", "kt"),
        ("left, a.kt icu then POSIX", LEFT, LEFT_ORACLE, "a", "kt"),
        ("left, b.kt icu then POSIX", LEFT, LEFT_ORACLE, "b", "kt"),
        ("left, a.g1 icu then POSIX", LEFT, LEFT_ORACLE, "a", "g1"),
    ] {
        let c = Case {
            name,
            ddl: ddl("und-x-icu", "und-x-icu", "default", "default")
                .replace("g1 text,", "g1 text collate \"und-x-icu\","),
            definition,
            oracle,
            recollate: &[],
        };
        let mut d = Driver::start(&c.ddl, COLUMNS, &[definition], &["public.a", "public.b"]).await;
        d.ctl
            .batch_execute(&alter(table, column, "POSIX"))
            .await
            .unwrap_or_else(|e| panic!("{name}: re-collate: {e}"));
        d.user()
            .await
            .batch_execute(
                "update public.a set v = v + 100 where id % 3 = 0; \
                 delete from public.a where id = 4; \
                 insert into public.a values (200, 'zz', 'k1', 'm1', 0, 5); \
                 insert into public.b values (20, 'k2', 'm2', 0, 7), (21, 'k9', 'm0', 1, 9); \
                 update public.b set y = y + 1 where id < 3; \
                 delete from public.b where id = 5",
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: changes: {e}"));
        assert_compares_clean(&d, "public.t", false, name).await;
        d.settle().await;
        c.assert_matches(&d, "applied").await;
    }
}

/// An `ON` column moved onto a nondeterministic collation, before the
/// staging worker's next pass pauses the definition for it: the target
/// part's own join compares the pair by its bytes, as the target was built,
/// so the key `'K1'` still matches nothing of `'k1'`, and no divergence is
/// reported that the collation's `=` would make. On one side and on both.
/// (A page of `M` or `J` fails until the pause: Postgres refuses the
/// substring search that renders a key's text under that collation.)
#[tokio::test]
async fn an_on_column_moved_onto_a_nondeterministic_collation_compares_by_bytes() {
    for (name, definition, recollate) in [
        ("inner, b.kt", INNER, &["b"][..]),
        ("left, b.kt", LEFT, &["b"][..]),
        ("left, a.kt and b.kt", LEFT, &["a", "b"][..]),
    ] {
        let d = Driver::start(
            &format!(
                "create collation public.ci (provider = icu, locale = 'und-u-ks-level2', \
                     deterministic = false); \
                 {}; \
                 insert into public.a values (100, 'x', 'K1', 'm1', 0, 1); \
                 insert into public.b values (100, 'K3', 'm1', 0, 5)",
                ddl("und-x-icu", "und-x-icu", "default", "default")
            ),
            COLUMNS,
            &[definition],
            &["public.a", "public.b"],
        )
        .await;
        assert_compares_clean(&d, "public.t", true, name).await;
        for table in recollate {
            d.ctl
                .batch_execute(&format!(
                    "alter table public.{table} alter column kt type text collate public.ci"
                ))
                .await
                .unwrap_or_else(|e| panic!("{name}: re-collate {table}: {e}"));
        }
        assert_compares_clean(&d, "public.t", true, name).await;
    }
}

// ---------------------------------------------------------------- 1-1 JOIN

/// A 1-1 `JOIN` on a text column of `b` (unique), re-collated on one side:
/// its check finds no divergence. The test applies no change after the
/// re-collation, which fails for the `FROM` side's (#1567).
#[tokio::test]
async fn a_one_to_one_join_on_column_re_collated() {
    let _serial = SERIAL.lock().await;
    for (name, table, definition, oracle) in [
        (
            "inner, a.kt icu then POSIX",
            "a",
            "TRANSFORM t FROM public.a JOIN public.b ON a.kt = b.kt \
             SELECT b.y AS y, a.v AS v",
            "select a.id, b.y, a.v from public.a a join public.b b on a.kt = b.kt collate \"C\"",
        ),
        (
            "left, b.kt icu then POSIX",
            "b",
            "TRANSFORM t FROM public.a LEFT JOIN public.b ON a.kt = b.kt \
             SELECT b.y AS y, a.v AS v",
            "select a.id, b.y, a.v from public.a a left join public.b b \
             on a.kt = b.kt collate \"C\"",
        ),
    ] {
        set_enabled(true);
        let d = Driver::start(
            "create table public.a (id integer primary key, kt text collate \"und-x-icu\", \
                 v integer); \
             create table public.b (id integer primary key, kt text collate \"und-x-icu\", \
                 y integer); \
             create unique index on public.b (kt); create index on public.a (kt); \
             insert into public.a select i, 'k' || i % 6, i from generate_series(1, 12) i; \
             insert into public.b select i, 'k' || i, i * 10 from generate_series(1, 4) i",
            &[("id", int4()), ("kt", ValueType::Text), ("v", int4())],
            &[definition],
            &["public.a", "public.b"],
        )
        .await;
        let actual = "select id, y, v from public.t order by id";
        let expected = format!("{oracle} order by 1");
        assert_eq!(
            d.rows(actual).await,
            d.rows(&expected).await,
            "{name}: built"
        );
        assert_converged(&d, "public.t", name).await;
        d.ctl
            .batch_execute(&alter(table, "kt", "POSIX"))
            .await
            .unwrap_or_else(|e| panic!("{name}: re-collate: {e}"));
        // The ledgers hold what the build copied; the check reads the
        // source tables as they are.
        assert_converged(&d, "public.t", name).await;
    }
}
