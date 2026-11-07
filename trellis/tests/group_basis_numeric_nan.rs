//! `SUM`/`AVG` over a `numeric(p,s)` column that holds `NaN` (#1756).
//!
//! A `numeric(p,s)` column admits `NaN` (never `±Infinity`: "cannot hold an
//! infinite value", on every supported Postgres). Postgres's `sum` and `avg`
//! are `NaN` while the group holds a `NaN` and the sum of the rest once it
//! leaves; `NaN` can't be subtracted back out of a running sum, so the
//! partials keep a `NaN` out of it and count it instead. Each step here
//! compares the target with Postgres's own aggregate by value, then
//! `self_check` must find nothing, on a plain aggregate and on an inner and a
//! `LEFT JOIN` (whose product multiplies a key's `NaN` count by its match
//! count, and a count by no match is no `NaN`).
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`
//! and never polls for convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::self_check::{
    SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn step(d: &mut Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

/// The rows of `actual` and `expected` (same columns) that the other lacks,
/// compared by value (`numeric`'s `=`: `NaN = NaN`, `1.5 = 1.50`).
async fn difference(d: &Driver, actual: &str, expected: &str) -> Vec<String> {
    d.rows(&format!(
        "(select 'target' as side, * from (({actual}) except all ({expected})) x) \
         union all \
         (select 'postgres' as side, * from (({expected}) except all ({actual})) y) \
         order by 1, 2"
    ))
    .await
}

/// Every divergence `self_check` reports on `target`, all pages.
async fn self_check(d: &Driver, target: &str) -> Vec<String> {
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = check_page(
            d.pool(),
            target,
            SelfCheckScope { after, limit: 100 },
            SelfCheckMode::Standard,
            Duration::from_secs(30),
        )
        .await
        .expect("a page of the check");
        match page.outcome {
            SelfCheckOutcome::Converged => {}
            SelfCheckOutcome::Diverged(divergences) => {
                found.extend(divergences.iter().map(|d| format!("{d:?}")));
            }
            other => panic!("unexpected outcome {other:?}"),
        }
        after = page.next_after;
        if after.is_none() {
            return found;
        }
    }
}

async fn definition_id(d: &Driver, target: &str) -> String {
    d.rows(&format!(
        "select id from transform_definitions where target_table = 'public.{target}'"
    ))
    .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string()
}

/// Runs `steps`, checking `targets` (target, its rows, Postgres's rows)
/// after each, and fails with every mismatch found on the way.
async fn run(d: &mut Driver, steps: &[&str], targets: &[(&str, &str, &str)]) {
    let mut failures = Vec::new();
    for sql in steps {
        step(d, sql).await;
        for (target, actual, expected) in targets {
            let diff = difference(d, actual, expected).await;
            if !diff.is_empty() {
                failures.push(format!("{target} after {sql}: {diff:?}"));
            }
            let found = self_check(d, target).await;
            if !found.is_empty() {
                failures.push(format!("{target} self_check after {sql}: {found:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

const SRC_DDL: &str = "create table public.src (id integer primary key, g integer, \
                                                x numeric(10, 2)); \
                       create index on public.src (g)";

const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
                   SELECT g AS g, SUM(x) AS total, AVG(x) AS mean, COUNT(x) AS cx, \
                          COUNT(*) AS n";

const AGG_ROWS: &str = "select g, total, mean, cx, n from public.agg";
const AGG_EXPECTED: &str =
    "select g, sum(x), avg(x), count(x), count(*) from public.src group by g";

async fn start_agg() -> Driver {
    Driver::start(
        SRC_DDL,
        &[("id", int4()), ("g", int4()), ("x", ValueType::Numeric)],
        &[AGG],
        &["public.src"],
    )
    .await
}

/// A `NaN` deleted, updated to a finite value, moved to another group and
/// back, two `NaN`s leaving one at a time, and a `NULL` beside them: the
/// target follows Postgres at every step.
#[tokio::test]
async fn a_nan_leaving_its_group_leaves_the_sum_of_the_rest() {
    let mut d = start_agg().await;
    run(
        &mut d,
        &[
            "insert into public.src values (1, 1, 1.50), (2, 1, 2.25), (3, 1, 'NaN'), \
                                           (4, 2, 4.00), (5, 1, null)",
            "delete from public.src where id = 3",
            "insert into public.src values (3, 1, 'NaN')",
            "update public.src set x = 3.00 where id = 3",
            "update public.src set x = 'NaN' where id = 3",
            "update public.src set g = 2 where id = 3",
            "update public.src set g = 1 where id = 3",
            "insert into public.src values (6, 1, 'NaN')",
            "delete from public.src where id = 3",
            "update public.src set x = -0.75 where id = 6",
            "update public.src set x = 'NaN', g = 2 where id = 1",
            "delete from public.src where id = 1",
        ],
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
    )
    .await;
}

/// A group whose rows were all `NaN` empties and its hidden row is
/// collected: nothing of the `NaN` is left behind.
#[tokio::test]
async fn a_group_that_held_only_nan_empties_and_is_collected() {
    let mut d = start_agg().await;
    run(
        &mut d,
        &[
            "insert into public.src values (1, 3, 'NaN'), (2, 3, 'NaN'), (3, 4, 1.00)",
            "delete from public.src where g = 3",
        ],
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
    )
    .await;
    d.settle().await;
    let id = definition_id(&d, "agg").await;
    let left = d
        .rows(&format!(
            "select g from {DEFAULT_SCHEMA}.agg_{id} where g = 3"
        ))
        .await;
    assert_eq!(left, Vec::<String>::new(), "the emptied group is collected");
}

/// A rebuild of the groups (`request_backfill`'s absolute write) of a
/// source that holds a `NaN` agrees with the incremental path, and a `NaN`
/// leaving after it still leaves the sum of the rest.
#[tokio::test]
async fn a_build_over_a_nan_then_the_nan_leaving() {
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, x numeric(10, 2)); \
         create index on public.src (g); \
         insert into public.src values (1, 1, 1.50), (2, 1, 'NaN'), (3, 2, 'NaN'), (4, 2, 2)",
        &[("id", int4()), ("g", int4()), ("x", ValueType::Numeric)],
        &[AGG],
        &["public.src"],
    )
    .await;
    run(
        &mut d,
        &[
            "select 1",
            "delete from public.src where id = 2",
            "update public.src set x = 5 where id = 3",
        ],
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
    )
    .await;
}

const JOIN_DDL: &str = "create table public.o (id integer primary key, k integer, \
                                               x numeric(10, 2)); \
                        create table public.c (id integer primary key, k integer, region text); \
                        create index on public.o (k); create index on public.c (k)";

const INNER: &str = "TRANSFORM ij FROM public.o JOIN public.c ON o.k = c.k GROUP BY c.region \
                     WITHOUT SOURCE INDEX SELECT c.region AS region, SUM(o.x) AS total, \
                            AVG(o.x) AS mean, COUNT(o.x) AS cx, COUNT(*) AS n";

const LEFT: &str = "TRANSFORM lj FROM public.o LEFT JOIN public.c ON o.k = c.k \
                    GROUP BY c.region \
                    WITHOUT SOURCE INDEX SELECT c.region AS region, SUM(o.x) AS total, \
                           AVG(o.x) AS mean, COUNT(o.x) AS cx, COUNT(*) AS n";

const INNER_ROWS: &str = "select region, total, mean, cx, n from public.ij";
const INNER_EXPECTED: &str = "select c.region, sum(o.x), avg(o.x), count(o.x), count(*) \
                              from public.o join public.c on o.k = c.k group by c.region";
const LEFT_ROWS: &str = "select region, total, mean, cx, n from public.lj";
const LEFT_EXPECTED: &str = "select c.region, sum(o.x), avg(o.x), count(o.x), count(*) \
                             from public.o left join public.c on o.k = c.k group by c.region";

/// On a `JOIN`: a `NaN` on a matched key leaving, a key holding a `NaN`
/// with no match (its `NaN` times no match is nothing), a match arriving
/// for it and leaving, and a `NaN` on a key whose match left but whose cell
/// is still there at zero.
#[tokio::test]
async fn join_groups_follow_a_nan_in_and_out_matched_or_not() {
    let mut d = Driver::start(
        JOIN_DDL,
        &[("id", int4()), ("k", int4()), ("x", ValueType::Numeric)],
        &[INNER, LEFT],
        &["public.o", "public.c"],
    )
    .await;
    run(
        &mut d,
        &[
            "insert into public.c values (1, 1, 'a'), (2, 2, 'b'), (3, 1, 'b')",
            "insert into public.o values (1, 1, 1.50), (2, 1, 'NaN'), (3, 2, 2.00), \
                                         (4, 9, 'NaN'), (5, 9, 7.00)",
            "delete from public.o where id = 2",
            "insert into public.o values (2, 1, 'NaN')",
            "update public.o set x = 0.50 where id = 2",
            // A match arrives for key 9, which holds a NaN, and leaves.
            "insert into public.c values (4, 9, 'b')",
            "delete from public.c where id = 4",
            // Key 2's match leaves; its cell stays at zero. A NaN arrives on
            // key 2, then a match for key 2 in 'b' again.
            "delete from public.c where id = 2",
            "insert into public.o values (6, 2, 'NaN')",
            "update public.o set x = 1.25 where id = 6",
            "insert into public.c values (2, 2, 'b')",
            "update public.o set x = 'NaN' where id = 6",
            "update public.o set k = 9 where id = 6",
            "delete from public.o where id in (4, 6)",
        ],
        &[
            ("ij", INNER_ROWS, INNER_EXPECTED),
            ("lj", LEFT_ROWS, LEFT_EXPECTED),
        ],
    )
    .await;
}

/// Every divergence and uncertain key one comparison of `target` reports,
/// all pages, with no convergence wait: what the target has applied against
/// the sources minus the ring rows not applied yet.
async fn compare(d: &Driver, target: &str) -> Vec<String> {
    let mut after: Option<String> = None;
    let mut found = Vec::new();
    loop {
        let page = compare_page(d.pool(), target, SelfCheckScope { after, limit: 100 })
            .await
            .expect("a comparison");
        found.extend(page.divergences.iter().map(|d| format!("{d:?}")));
        if page.uncertain_all || !page.uncertain_keys.is_empty() {
            found.push(format!("uncertain: {:?}", page.uncertain_keys));
        }
        after = page.next_after;
        if after.is_none() {
            return found;
        }
    }
}

/// A `NaN` arriving, leaving, or changing value in changes no drain has
/// applied yet is subtracted from the sources by value: the comparison finds
/// nothing on a correct target, on a plain aggregate and both `JOIN`s.
#[tokio::test]
async fn a_nan_not_applied_yet_is_subtracted_from_the_sources() {
    let mut d = Driver::start(
        &format!("{SRC_DDL}; {JOIN_DDL}"),
        &[("id", int4()), ("g", int4()), ("x", ValueType::Numeric)],
        &[AGG, INNER, LEFT],
        &["public.src", "public.o", "public.c"],
    )
    .await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 1.50), (2, 1, 'NaN'), (3, 2, 2); \
         insert into public.c values (1, 1, 'a'), (2, 2, 'b'); \
         insert into public.o values (1, 1, 1.50), (2, 1, 'NaN'), (3, 2, 2), (4, 9, 'NaN')",
    )
    .await;
    for pending in [
        "delete from public.src where id = 2; delete from public.o where id = 2",
        "insert into public.src values (4, 2, 'NaN'); insert into public.o values (5, 2, 'NaN')",
        "update public.src set x = 'NaN' where id = 1; update public.o set x = 'NaN' where id = 1",
        "update public.src set x = 7 where id = 4; update public.o set x = 7 where id = 4; \
         insert into public.c values (3, 9, 'a')",
    ] {
        d.user().await.batch_execute(pending).await.expect(pending);
        for target in ["agg", "ij", "lj"] {
            assert_eq!(
                compare(&d, target).await,
                Vec::<String>::new(),
                "{target}: {pending}"
            );
        }
        d.settle().await;
        for target in ["agg", "ij", "lj"] {
            assert_eq!(
                self_check(&d, target).await,
                Vec::<String>::new(),
                "{target}: {pending}"
            );
        }
    }
}
