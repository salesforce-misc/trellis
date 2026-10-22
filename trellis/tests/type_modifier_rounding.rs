//! A source column whose type modifier the source widens in the catalog only
//! (`timestamp(0)` -> `timestamp(3)`, `numeric(6,2)` -> `numeric`, an
//! `interval` widening, `varbit(5)` -> `varbit(8)`) is copied with no rounding
//! (#1288; the research is #1224). Postgres rounds silently when it
//! *assigns* a value into a `numeric(p,s)`, `timestamp(p)`, `timestamptz(p)`,
//! `time(p)`, `timetz(p)` or `interval` column, so the "never cast to a bounded
//! type" rule of #1207 does not protect a copy that keeps the source's old
//! modifier. A catalog-only widening rewrites nothing, so no rebuild follows;
//! the copy keeps the old modifier until the next capture pass pauses the
//! definition, and the values drained before it are rounded.
//!
//! Each test creates the source with the narrow type and seeds a row, widens
//! the column, writes a value only the wide type can hold (and, for a key, a
//! second key the narrow type rounds onto the first), and drains with **no
//! pass in between**. Then it runs one pass and drains again. It prints the
//! definition's status, its pause reason and the `self_check` verdict before
//! and after the pass on stderr (`--nocapture`, lines starting `REPORT`).
//! The pipeline and the pass are stepped by hand (`support/drain_driver.rs`);
//! nothing polls for convergence (#297).
//!
//! The `cases!` table is place x type. Every cell is a catalog-only
//! widening, and every cell expects the same outcome ([`exact`], the rule
//! #1224's research proposes): the target equals the source before and
//! after the pass, `self_check` agrees, and nothing pauses or is held. A cell
//! that fails on main is `#[ignore = "known gap #1288"]`; the fix turns it
//! green. The `detected!` guards are live: for one cell per place they pin
//! how the gap shows today ([`detected_today`]), which is what keeps it a
//! detected gap rather than a silent one.
//!
//! Not in the table:
//! - widenings that rewrite the source (`numeric(6,2)` -> `numeric(8,4)`, any
//!   change of an array's element modifier): the rewrite rebuild covers them,
//!   and their window is the existing rewrite gap;
//! - the keys define refuses: `interval` as any key, `numeric` as a `JOIN
//!   ON` key;
//! - the ledger `GROUP BY` path: its copies are family-typed like the
//!   group-basis ones, and it needs the group-basis knob off, which is
//!   process-global, so it needs its own test binary.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{PgType, ValueType};
use trellis::integer::IntWidth;
use trellis::staging::self_check::CheckPageExt;
use trellis::staging::{StagedWatermark, apply};
use trellis::{Config, SelfCheckMode, Trellis, TrellisOptions};

/// One type and the widening of it.
struct Case {
    /// The test's label in the report.
    label: &'static str,
    /// The source column's type, and what the DDL changes it to.
    narrow: &'static str,
    wide: &'static str,
    /// A value the narrow type holds exactly.
    seed: &'static str,
    /// A value only the wide type holds exactly (the narrow type rounds it
    /// to `seed`, or to something else the narrow type holds).
    hi: &'static str,
    /// The `ValueType` the engine classifies the column as.
    value_type: ValueType,
}

const TS0: Case = Case {
    label: "timestamp(0) -> timestamp(3)",
    narrow: "timestamp(0)",
    wide: "timestamp(3)",
    seed: "2024-01-01 00:00:00",
    hi: "2024-01-01 00:00:00.4",
    value_type: ValueType::Other(PgType::Timestamp),
};
const TS0_UNBOUNDED: Case = Case {
    label: "timestamp(0) -> timestamp",
    narrow: "timestamp(0)",
    wide: "timestamp",
    seed: "2024-01-01 00:00:00",
    hi: "2024-01-01 00:00:00.123456",
    value_type: ValueType::Other(PgType::Timestamp),
};
const TSTZ0: Case = Case {
    label: "timestamptz(0) -> timestamptz(3)",
    narrow: "timestamptz(0)",
    wide: "timestamptz(3)",
    seed: "2024-01-01 00:00:00+00",
    hi: "2024-01-01 00:00:00.4+00",
    value_type: ValueType::Other(PgType::TimestampTz),
};
const TIME0: Case = Case {
    label: "time(0) -> time(3)",
    narrow: "time(0)",
    wide: "time(3)",
    seed: "10:00:00",
    hi: "10:00:00.4",
    value_type: ValueType::Other(PgType::Time),
};
const TIMETZ0: Case = Case {
    label: "timetz(0) -> timetz(3)",
    narrow: "timetz(0)",
    wide: "timetz(3)",
    seed: "10:00:00+00",
    hi: "10:00:00.4+00",
    value_type: ValueType::Other(PgType::TimeTz),
};
const NUMERIC: Case = Case {
    label: "numeric(6,2) -> numeric",
    narrow: "numeric(6,2)",
    wide: "numeric",
    seed: "1.25",
    hi: "1.23456789",
    value_type: ValueType::Numeric,
};
const INTERVAL0: Case = Case {
    label: "interval(0) -> interval(3)",
    narrow: "interval(0)",
    wide: "interval(3)",
    seed: "1 day 00:00:01",
    hi: "1 day 00:00:01.4",
    value_type: ValueType::Other(PgType::Interval),
};
const INTERVAL_FIELDS: Case = Case {
    label: "interval day -> interval day to second",
    narrow: "interval day",
    wide: "interval day to second",
    seed: "1 day",
    hi: "1 day 00:00:01.5",
    value_type: ValueType::Other(PgType::Interval),
};
const INTERVAL_YM: Case = Case {
    label: "interval year to month -> interval",
    narrow: "interval year to month",
    wide: "interval",
    seed: "1 year 2 mons",
    hi: "1 year 2 mons 3 days",
    value_type: ValueType::Other(PgType::Interval),
};
const INTERVAL_HOUR: Case = Case {
    label: "interval hour -> interval minute",
    narrow: "interval hour",
    wide: "interval minute",
    seed: "02:00:00",
    hi: "02:30:00",
    value_type: ValueType::Other(PgType::Interval),
};
const VARBIT: Case = Case {
    label: "varbit(5) -> varbit(8)",
    narrow: "bit varying(5)",
    wide: "bit varying(8)",
    seed: "10101",
    hi: "10101000",
    value_type: ValueType::Other(PgType::VarBit),
};

/// What Trellis reported after one case.
struct Outcome {
    /// How many drain attempts failed before a page committed (a copy that
    /// refuses a value errors the drain and charges the key a death).
    drain_failures: usize,
    /// The status and `self_check` verdict after the drain and before any
    /// pass.
    status_before_pass: String,
    self_check_before_pass: String,
    /// Rows of the target that differ from the source before any pass.
    diverged_before_pass: Vec<String>,
    /// Rows of the target that differ from the source after the pass and a
    /// drain.
    diverged_after_pass: Vec<String>,
    /// `"live"`, or `"paused: <reason>"` after the pass.
    status: String,
    /// `self_check`'s verdict after the pass and a drain.
    self_check: String,
}

impl Outcome {
    fn report(&self, place: &str, case: &Case) {
        eprintln!(
            "REPORT {place} | {} | before pass: {} failed drains, status {}, self_check {}, \
             diverged {:?} | after pass: status {} diverged {:?} | self_check {}",
            case.label,
            self.drain_failures,
            self.status_before_pass,
            self.self_check_before_pass,
            self.diverged_before_pass,
            self.status,
            self.diverged_after_pass,
            self.self_check
        );
    }
}

async fn pass(d: &mut Driver) {
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(
        &mut d.ctl,
        &pool,
        DEFAULT_SCHEMA,
        "type_modifier_rounding_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

async fn facade(d: &Driver) -> Trellis {
    let config = Config::with_schema(d.db.dsn().to_string(), DEFAULT_SCHEMA).expect("config");
    Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect")
}

async fn status_of(trellis: &Trellis, target: &str) -> String {
    let reported = trellis
        .status(target)
        .await
        .expect("status")
        .expect("the definition");
    let held = match &reported.held_keys {
        Some(held) => format!(" (held keys: {})", held.count),
        None => String::new(),
    };
    match reported.capture_failure {
        Some(failure) => format!("{:?}: {}{held}", reported.status, failure.error),
        None => format!("{:?}{held}", reported.status),
    }
}

/// `self_check`'s verdict. Nothing drains in the background, so whether the
/// target has caught up is settled before the call: the timeout only bounds
/// how long a held key (which never catches up) takes to report
/// `NotCaughtUp`.
async fn self_check_of(trellis: &Trellis, target: &str) -> String {
    match trellis
        .self_check_to_end(target, SelfCheckMode::Strict, Duration::from_secs(3))
        .await
    {
        Ok(report) => format!("{:?}", report.outcome),
        Err(err) => format!("error: {err}"),
    }
}

/// The columns of the table `by` (the oracle's `sql` shape) that differ.
async fn diverged(d: &Driver, target_sql: &str, source_sql: &str) -> Vec<String> {
    d.rows(&format!(
        "(({target_sql}) except ({source_sql})) union all (({source_sql}) except ({target_sql}))"
    ))
    .await
}

/// Seals and drains eight times, tolerating a drain that fails because a
/// copy refuses a value: each failure charges the key a death, so the page
/// commits once the key is held. A fixed number of rounds with no sleep and
/// no exit on a condition, so it is not a wait for convergence (#297).
/// Returns how many attempts failed.
async fn settle_tolerant(d: &mut Driver) -> usize {
    let mut failures = 0;
    for _ in 0..8 {
        let batch = d.seal().await;
        loop {
            match apply::drain_once(
                d.pool(),
                batch,
                "worker",
                1,
                drain_driver::WAKE,
                &StagedWatermark::saturated(),
            )
            .await
            {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(err) => {
                    failures += 1;
                    assert!(failures <= 40, "the batch never drained: {err}");
                }
            }
        }
        d.retire().await;
    }
    failures
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// One scenario: a source, one definition named `c`, a seed, a catalog-only
/// widening, a write only the wide type holds, and the SQL that reads the
/// target and its oracle (the source) in the same shape.
struct Scenario {
    ddl: String,
    columns: Vec<(&'static str, ValueType)>,
    definition: &'static str,
    captured: &'static [&'static str],
    seed_sql: String,
    widen_sql: String,
    hi_sql: String,
    target_sql: &'static str,
    oracle_sql: String,
}

async fn run(scn: Scenario) -> Outcome {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(&scn.ddl, &scn.columns, &[scn.definition], scn.captured).await;
    write(&d, &scn.seed_sql).await;
    d.settle().await;
    d.ctl.batch_execute(&scn.widen_sql).await.expect("widen");
    write(&d, &scn.hi_sql).await;
    let drain_failures = settle_tolerant(&mut d).await;
    let diverged_before_pass = diverged(&d, scn.target_sql, &scn.oracle_sql).await;
    let trellis = facade(&d).await;
    let status_before_pass = status_of(&trellis, "c").await;
    let self_check_before_pass = self_check_of(&trellis, "c").await;

    pass(&mut d).await;
    settle_tolerant(&mut d).await;
    let status = status_of(&trellis, "c").await;
    let self_check = self_check_of(&trellis, "c").await;
    let diverged_after_pass = diverged(&d, scn.target_sql, &scn.oracle_sql).await;
    trellis.shutdown().await.expect("shutdown");
    Outcome {
        drain_failures,
        status_before_pass,
        self_check_before_pass,
        diverged_before_pass,
        diverged_after_pass,
        status,
        self_check,
    }
}

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// A 1-1 target over `t(id integer primary key, v <T>, n integer)` copying
/// `v` as a passthrough.
fn passthrough(case: &Case) -> Scenario {
    Scenario {
        ddl: format!(
            "create table public.t (id integer primary key, v {}, n integer)",
            case.narrow
        ),
        columns: vec![("id", int4()), ("v", case.value_type), ("n", int4())],
        definition: "TRANSFORM c FROM public.t SELECT v AS v, n AS n",
        captured: &["public.t"],
        seed_sql: format!("insert into public.t values (1, '{}', 1)", case.seed),
        widen_sql: format!("alter table public.t alter column v type {}", case.wide),
        hi_sql: format!("insert into public.t values (2, '{}', 2)", case.hi),
        target_sql: "select id, v::text, n from public.c",
        oracle_sql: "select id, v::text, n from public.t".into(),
    }
}

/// A 1-1 target over `t(k <T> primary key, n integer)`, keyed by `k`. The
/// narrow type rounds the second key onto the first.
fn primary_key(case: &Case) -> Scenario {
    Scenario {
        ddl: format!(
            "create table public.t (k {} primary key, n integer)",
            case.narrow
        ),
        columns: vec![("k", case.value_type), ("n", int4())],
        definition: "TRANSFORM c FROM public.t SELECT n AS n",
        captured: &["public.t"],
        seed_sql: format!("insert into public.t values ('{}', 1)", case.seed),
        widen_sql: format!("alter table public.t alter column k type {}", case.wide),
        hi_sql: format!("insert into public.t values ('{}', 2)", case.hi),
        target_sql: "select k::text, n from public.c",
        oracle_sql: "select k::text, n from public.t".into(),
    }
}

/// A group-basis aggregate over `t(id integer primary key, g <T>, x
/// integer)` grouped by `g`. Its group copies are made at the family type.
fn group_by(case: &Case) -> Scenario {
    Scenario {
        ddl: format!(
            "create table public.t (id integer primary key, g {0}, x integer); \
             create index on public.t (g)",
            case.narrow
        ),
        columns: vec![("id", int4()), ("g", case.value_type), ("x", int4())],
        definition: "TRANSFORM c FROM public.t GROUP BY g \
                     SELECT g AS g, COUNT(*) AS n, SUM(x) AS sx",
        captured: &["public.t"],
        seed_sql: format!("insert into public.t values (1, '{}', 1)", case.seed),
        widen_sql: format!("alter table public.t alter column g type {}", case.wide),
        hi_sql: format!("insert into public.t values (2, '{}', 2)", case.hi),
        target_sql: "select g::text, n, sx from public.c",
        oracle_sql: "select g::text, count(*)::bigint, sum(x)::bigint from public.t group by g"
            .into(),
    }
}

/// A `JOIN` on `a.k = b.k` of two tables whose `k` is `<T>`, grouped by `k`.
/// Both tables widen `k`.
fn join_on(case: &Case) -> Scenario {
    Scenario {
        ddl: format!(
            "create table public.a (id integer primary key, k {0}, x integer); \
             create table public.b (id integer primary key, k {0}, y integer); \
             create index on public.a (k); create index on public.b (k)",
            case.narrow
        ),
        columns: vec![
            ("id", int4()),
            ("k", case.value_type),
            ("x", int4()),
            ("y", int4()),
        ],
        definition: "TRANSFORM c FROM public.a LEFT JOIN public.b ON a.k = b.k \
                     GROUP BY a.k SELECT a.k AS k, COUNT(*) AS n, SUM(b.y) AS sy",
        captured: &["public.a", "public.b"],
        seed_sql: format!(
            "insert into public.a values (1, '{0}', 1); insert into public.b values (1, '{0}', 10)",
            case.seed
        ),
        widen_sql: format!(
            "alter table public.a alter column k type {0}; alter table public.b alter column k type {0}",
            case.wide
        ),
        hi_sql: format!(
            "insert into public.a values (2, '{0}', 2); insert into public.b values (2, '{0}', 20)",
            case.hi
        ),
        target_sql: "select k::text, n, sy from public.c",
        oracle_sql: "select a.k::text, count(*)::bigint, sum(b.y)::bigint \
                     from public.a left join public.b on a.k = b.k group by a.k"
            .into(),
    }
}

/// A `JOIN` on an integer, grouped by a `<T>` column of the left table.
fn join_group_by(case: &Case) -> Scenario {
    Scenario {
        ddl: format!(
            "create table public.a (id integer primary key, g {}, j integer); \
             create table public.b (id integer primary key, j integer, y integer); \
             create index on public.a (j); create index on public.b (j)",
            case.narrow
        ),
        columns: vec![
            ("id", int4()),
            ("g", case.value_type),
            ("j", int4()),
            ("y", int4()),
        ],
        definition: "TRANSFORM c FROM public.a LEFT JOIN public.b ON a.j = b.j \
                     GROUP BY a.g SELECT a.g AS g, COUNT(*) AS n, SUM(b.y) AS sy",
        captured: &["public.a", "public.b"],
        seed_sql: format!(
            "insert into public.a values (1, '{}', 1); insert into public.b values (1, 1, 10)",
            case.seed
        ),
        widen_sql: format!("alter table public.a alter column g type {}", case.wide),
        hi_sql: format!(
            "insert into public.a values (2, '{}', 2); insert into public.b values (2, 2, 20)",
            case.hi
        ),
        target_sql: "select g::text, n, sy from public.c",
        oracle_sql: "select a.g::text, count(*)::bigint, sum(b.y)::bigint \
                     from public.a left join public.b on a.j = b.j group by a.g"
            .into(),
    }
}

/// Runs `scn`, prints what Trellis reported, and asserts every cell's
/// expected-correct outcome for a catalog-only widening (#1224's research,
/// section 4): the target equals the source after the drain that precedes
/// any pass (no value rounded, no two keys merged), `self_check` agrees, and
/// the pass changes nothing: the definition stays live, nothing is held and
/// the target still equals the source.
async fn exact(place: &str, case: Case, scn: Scenario) {
    let outcome = run(scn).await;
    outcome.report(place, &case);
    let label = case.label;
    assert_eq!(
        outcome.diverged_before_pass,
        Vec::<String>::new(),
        "{place}, {label}: the target differs from the source before the pass (a rounded \
         value or merged keys); status {}, self_check {}",
        outcome.status_before_pass,
        outcome.self_check_before_pass
    );
    assert_eq!(
        (
            outcome.drain_failures,
            outcome.status_before_pass.as_str(),
            outcome.self_check_before_pass.as_str()
        ),
        (0, "Live", "Converged"),
        "{place}, {label}: before the pass"
    );
    assert_eq!(
        (outcome.status.as_str(), outcome.self_check.as_str()),
        ("Live", "Converged"),
        "{place}, {label}: the pass paused or held something for a widening every copy holds"
    );
    assert_eq!(
        outcome.diverged_after_pass,
        Vec::<String>::new(),
        "{place}, {label}: after the pass"
    );
}

/// Pins how #1288's window shows today, for a cell [`exact`] fails: what
/// makes it a detected gap rather than a silent one. Before the pass the
/// status still says live (the silent part) while `self_check` already
/// reports the difference; the pass then pauses the definition for the
/// widened column, `self_check` says the definition isn't live, and the
/// rounded rows stay in the target (a `RESUME` rebuilds them). If the fix
/// makes the cell exact, this fails: delete the guard and un-ignore the
/// cell. If the pass stopped pausing, the gap would be silent and
/// undetected and belong in `docs/known-correctness-gaps.md`.
async fn detected_today(place: &str, case: Case, scn: Scenario) {
    let outcome = run(scn).await;
    outcome.report(place, &case);
    let label = case.label;
    assert_ne!(
        outcome.diverged_before_pass,
        Vec::<String>::new(),
        "{place}, {label}: the target is exact: #1288 is fixed here, so delete this guard and \
         un-ignore the cell"
    );
    if outcome.drain_failures == 0 {
        // A rounded value: written silently.
        assert_eq!(outcome.status_before_pass, "Live", "{place}, {label}");
        assert!(
            outcome.self_check_before_pass.starts_with("Diverged("),
            "{place}, {label}: self_check before the pass: {}",
            outcome.self_check_before_pass
        );
    } else {
        // A copy refusing the value (`varbit`): the key is held.
        assert_eq!(
            (
                outcome.status_before_pass.as_str(),
                outcome.self_check_before_pass.as_str()
            ),
            ("Live (held keys: 1)", "NotCaughtUp"),
            "{place}, {label}"
        );
    }
    assert!(
        outcome.status.starts_with("Paused: column ")
            && outcome.status.contains(" widened to ")
            && outcome
                .status
                .contains("Trellis keeps a copy of it that can't hold every value of that type"),
        "{place}, {label}: the pass did not pause the definition for the widening: {}",
        outcome.status
    );
    assert_eq!(outcome.self_check, "NotLive(Paused)", "{place}, {label}");
    assert_eq!(
        outcome.diverged_after_pass, outcome.diverged_before_pass,
        "{place}, {label}: the pause changed the target"
    );
}

/// One test per cell of the place x type table, each asserting [`exact`].
/// Every cell here is a catalog-only widening, so every cell's expected
/// outcome is the same: [`exact`]. A cell that fails on main carries
/// `#[ignore = "known gap #1288"]`; the fix removes it. Extending the table
/// (#1289's generated matrix) is adding a [`Case`] and a row; a change kind
/// with another expected outcome (a rewrite, a narrowing) needs its own
/// assertion beside [`exact`].
macro_rules! cases {
    ($($name:ident: $place:literal, $case:ident, $scenario:ident $(, $ignore:literal)?;)*) => {$(
        #[tokio::test]
        $(#[ignore = $ignore])?
        async fn $name() {
            exact($place, $case, $scenario(&$case)).await;
        }
    )*};
}

/// A live guard per place (and the held-key path) that [`detected_today`]
/// holds for a cell [`cases!`] ignores.
macro_rules! detected {
    ($($name:ident: $place:literal, $case:ident, $scenario:ident;)*) => {$(
        #[tokio::test]
        async fn $name() {
            detected_today($place, $case, $scenario(&$case)).await;
        }
    )*};
}

detected! {
    detected_ts_passthrough: "1-1 passthrough", TS0, passthrough;
    detected_numeric_passthrough: "1-1 passthrough", NUMERIC, passthrough;
    detected_varbit_passthrough: "1-1 passthrough", VARBIT, passthrough;
    detected_ts_primary_key: "1-1 primary key", TS0, primary_key;
    detected_ts_join_on: "JOIN ON + GROUP BY", TS0, join_on;
    detected_numeric_join_group_by: "JOIN GROUP BY", NUMERIC, join_group_by;
}

cases! {
    ts_passthrough: "1-1 passthrough", TS0, passthrough, "known gap #1288";
    ts_unbounded_passthrough: "1-1 passthrough", TS0_UNBOUNDED, passthrough, "known gap #1288";
    tstz_passthrough: "1-1 passthrough", TSTZ0, passthrough, "known gap #1288";
    time_passthrough: "1-1 passthrough", TIME0, passthrough, "known gap #1288";
    timetz_passthrough: "1-1 passthrough", TIMETZ0, passthrough, "known gap #1288";
    numeric_passthrough: "1-1 passthrough", NUMERIC, passthrough, "known gap #1288";
    interval_precision_passthrough: "1-1 passthrough", INTERVAL0, passthrough, "known gap #1288";
    interval_fields_passthrough: "1-1 passthrough", INTERVAL_FIELDS, passthrough, "known gap #1288";
    varbit_passthrough: "1-1 passthrough", VARBIT, passthrough, "known gap #1288";
    interval_ym_passthrough: "1-1 passthrough", INTERVAL_YM, passthrough, "known gap #1288";
    interval_hour_passthrough: "1-1 passthrough", INTERVAL_HOUR, passthrough, "known gap #1288";

    ts_primary_key: "1-1 primary key", TS0, primary_key, "known gap #1288";
    ts_unbounded_primary_key: "1-1 primary key", TS0_UNBOUNDED, primary_key, "known gap #1288";
    tstz_primary_key: "1-1 primary key", TSTZ0, primary_key, "known gap #1288";
    time_primary_key: "1-1 primary key", TIME0, primary_key, "known gap #1288";
    timetz_primary_key: "1-1 primary key", TIMETZ0, primary_key, "known gap #1288";
    varbit_primary_key: "1-1 primary key", VARBIT, primary_key, "known gap #1288";

    ts_group_by: "group-basis GROUP BY", TS0, group_by;
    tstz_group_by: "group-basis GROUP BY", TSTZ0, group_by;
    numeric_group_by: "group-basis GROUP BY", NUMERIC, group_by;
    time_group_by: "group-basis GROUP BY", TIME0, group_by;
    varbit_group_by: "group-basis GROUP BY", VARBIT, group_by;

    ts_join_on: "JOIN ON + GROUP BY", TS0, join_on, "known gap #1288";
    tstz_join_on: "JOIN ON + GROUP BY", TSTZ0, join_on, "known gap #1288";
    varbit_join_on: "JOIN ON + GROUP BY", VARBIT, join_on, "known gap #1288";
    ts_join_group_by: "JOIN GROUP BY", TS0, join_group_by, "known gap #1288";
    tstz_join_group_by: "JOIN GROUP BY", TSTZ0, join_group_by, "known gap #1288";
    time_join_group_by: "JOIN GROUP BY", TIME0, join_group_by, "known gap #1288";
    numeric_join_group_by: "JOIN GROUP BY", NUMERIC, join_group_by, "known gap #1288";
    varbit_join_group_by: "JOIN GROUP BY", VARBIT, join_group_by, "known gap #1288";
}
