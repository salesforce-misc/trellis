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
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::self_check::{
    SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};
use trellis::{Trellis, TrellisOptions};

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

/// The widest `numeric(p,s)` there is, `numeric(1000, -1000)` (values below
/// `10^2000`), beside `NaN`s, read by fields composed over the `SUM` and
/// `AVG`: the finite sum never reaches the `NaN` weight, and every composed
/// field sees `NaN` exactly while Postgres does.
#[tokio::test]
async fn the_widest_numeric_and_fields_composed_over_the_sum() {
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, x numeric(1000, -1000)); \
         create index on public.src (g)",
        &[("id", int4()), ("g", int4()), ("x", ValueType::Numeric)],
        &[
            "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
           SELECT g AS g, SUM(x) AS total, SUM(x) / 2 AS half, SUM(x) + 1 AS plus, \
                  AVG(x) * 2 AS twice, COALESCE(SUM(x), 0) AS zeroed, COUNT(*) AS n",
        ],
        &["public.src"],
    )
    .await;
    let big = format!("9{}", "0".repeat(1999));
    let rows = "select g, total, half, plus, twice, zeroed, n from public.agg";
    let expected = "select g, sum(x), sum(x) / 2, sum(x) + 1, avg(x) * 2, \
                    coalesce(sum(x), 0), count(*) from public.src group by g";
    let steps = [
        format!(
            "insert into public.src values (1, 1, {big}), (2, 1, {big}), (3, 1, 'NaN'), \
                                           (4, 2, -{big}), (5, 2, 'NaN'), (6, 2, 'NaN')"
        ),
        "delete from public.src where id = 3".to_string(),
        "update public.src set x = -x where id = 1".to_string(),
        "delete from public.src where id = 5".to_string(),
        format!("update public.src set x = {big} where id = 6"),
        "update public.src set x = 'NaN' where id in (1, 2, 4)".to_string(),
        "update public.src set x = null where id in (1, 2)".to_string(),
    ];
    let steps: Vec<&str> = steps.iter().map(String::as_str).collect();
    run(&mut d, &steps, &[("agg", rows, expected)]).await;
}

/// A `NaN` matched twice in one `JOIN` group counts twice, and the group
/// shows the sum of the rest once both matches, or the `NaN`, leave.
#[tokio::test]
async fn a_nan_matched_twice_in_one_join_group() {
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
            "insert into public.c values (1, 1, 'a'), (2, 1, 'a'), (3, 2, 'a')",
            "insert into public.o values (1, 1, 'NaN'), (2, 1, 1.25), (3, 2, 'NaN'), (4, 2, 3)",
            "delete from public.c where id = 1",
            "delete from public.c where id = 3",
            "insert into public.c values (1, 1, 'a'), (3, 2, 'a')",
            "delete from public.o where id = 1",
            "update public.c set region = 'b' where id = 2",
            "update public.o set x = 2 where id = 3",
        ],
        &[
            ("ij", INNER_ROWS, INNER_EXPECTED),
            ("lj", LEFT_ROWS, LEFT_EXPECTED),
        ],
    )
    .await;
}

/// A hidden sum stored as `NaN` itself, as a release that summed a `NaN`
/// into it left it: the group reads `NaN` after the `NaN` leaves, and
/// `self_check` reports the sum; a rebuild (`request_backfill`) rewrites it
/// counted. A wrong sum of a group that counts a `NaN` is reported with the
/// count apart, in a few digits.
#[tokio::test]
async fn a_sum_stored_as_nan_is_reported_and_a_rebuild_repairs_it() {
    let mut d = start_agg().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 1.50), (2, 1, 2.25), (3, 1, 'NaN'), (4, 2, 1)",
    )
    .await;
    let id = definition_id(&d, "agg").await;
    let hidden = format!("{DEFAULT_SCHEMA}.agg_{id}");
    d.ctl
        .batch_execute(&format!(
            "update {hidden} set __sum_1 = __sum_1 + 1 where g = 1"
        ))
        .await
        .expect("plant a wrong sum");
    let found = self_check(&d, "agg").await;
    assert!(
        found.iter().any(|f| f.contains(&format!(
            "agg_{id}.__sum_1\", persisted: Some(\"4.75 (+ 1 NaN)\"), \
             recomputed: Some(\"3.75 (+ 1 NaN)\")"
        ))),
        "{found:?}"
    );

    d.ctl
        .batch_execute(&format!("update {hidden} set __sum_1 = 'NaN' where g = 1"))
        .await
        .expect("a sum stored as NaN");
    step(&mut d, "delete from public.src where id = 3").await;
    assert_eq!(
        d.rows("select total from public.agg where g = 1").await,
        ["(NaN)"],
        "a sum stored as NaN keeps reading NaN"
    );
    let found = self_check(&d, "agg").await;
    assert!(
        found.iter().any(|f| f.contains(&format!(
            "agg_{id}.__sum_1\", persisted: Some(\"NaN\"), recomputed: Some(\"3.75\")"
        ))),
        "{found:?}"
    );

    let trellis = Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    trellis
        .request_backfill("src")
        .await
        .expect("request a rebuild");
    d.run_frozen(&[], |pool| async move {
        let claimed = chunk_queue::claim_chunks_of(
            &**pool.get().await.expect("pool"),
            "rebuild",
            1,
            &[chunk_queue::KIND_SWEEP],
        )
        .await
        .expect("claim the rebuild");
        assert_eq!(claimed.len(), 1, "one rebuild job to claim");
        let options = WorkerOptions {
            chunk_rows: 1_000,
            drain_batch_cap: usize::MAX,
            heartbeat_interval: Duration::from_secs(5),
            reclaim_ttl: Duration::from_secs(60),
        };
        build::run_claimed(&pool, &claimed[0], "rebuild", &options).await;
        Ok(())
    })
    .await
    .finish()
    .await;
    assert_eq!(
        difference(&d, AGG_ROWS, AGG_EXPECTED).await,
        Vec::<String>::new()
    );
    assert_eq!(self_check(&d, "agg").await, Vec::<String>::new());
    run(
        &mut d,
        &[
            "insert into public.src values (3, 1, 'NaN')",
            "delete from public.src where id = 3",
        ],
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
    )
    .await;
}
