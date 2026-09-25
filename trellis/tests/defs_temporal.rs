//! End-to-end tests for issue #113's temporal types (`date`, `time`,
//! `timetz`, `timestamp`, `timestamptz`, `interval`) against a real,
//! ephemeral Postgres via `testkit::TestCluster` — including issue #248's
//! fix, which reconciled `to_jsonb`'s row-body renderer with `::text` and
//! re-admitted `timestamp` to every key/`MIN`/`MAX` role `date`/`time`/
//! `timetz` already held, and issue #246's fix, which pinned the walsender's
//! output GUCs (including, now, `TimeZone`) the same way the pool's already
//! were and let `timestamptz` join them too.
//!
//! # Why these run against a live server
//!
//! `docs/type-support.md` had every temporal row marked `🎯 typed index`,
//! on the premise that #110's typed key index was the prerequisite for any
//! key role. #111 found that premise wrong for `oid` and #112 found it
//! right for floats, so #113 treats it as a question to be *asked of a
//! server*, per family, rather than answered from the block's name. Every
//! claim below is therefore a claim about agreeing with a real Postgres,
//! and is checked against one:
//!
//! 1. **Text stability.** For `date`/`timestamp`/`time`/`timetz`, equal
//!    values must render identically and distinct values distinctly — the
//!    exact property raw-`::text` key matching needs. Demonstrated by
//!    asking the server to `group by` a spread of values and comparing the
//!    group count against the distinct-`::text` count.
//! 2. **The one remaining refusal, demonstrated not asserted.** `interval`
//!    is refused every key role because `'24 hours' = '1 day'` is **true**
//!    while their `::text` differs; the test reads both facts out of the
//!    server. `timestamptz` used to be refused too, because its rendering
//!    moved with `TimeZone` on a walsender Trellis couldn't pin — issue
//!    #246 closed that (independently of #248), and
//!    `timestamptz_text_moves_with_the_session_timezone` below still shows
//!    the underlying hazard `TimeZone` pinning now defends against.
//! 3. **Comparison order.** `trellis::temporal::compare` must agree with
//!    each family's own `<`/`=`/`>` over a grid that includes BC years,
//!    `infinity`, sub-second fractions, `24:00:00`, and `timetz`'s
//!    surprising GMT-then-zone tie-break.
//! 4. **Aggregate result types.** `min`/`max` keep their argument's type
//!    and `sum(interval)` is `interval`, checked against `pg_typeof`.
//! 5. **`interval` arithmetic fidelity.** `trellis::temporal::Interval`'s
//!    addition and `interval_out` rendering are compared byte-for-byte
//!    against the server's own, including the 30-day-month comparison rule,
//!    the no-justification addition rule, and the overflow error.
//! 6. **Why `MIN`/`MAX(interval)` is refused.** Demonstrated from the
//!    server: `max(v)` over the same three rows returns *different text*
//!    for two different scan orders, so it is not a function of its input
//!    and ADR-0013's byte-exact recompute cross-check could never settle
//!    it.
//! 7. **Issue #248's fix, both directly and end-to-end.**
//!    `the_engine_s_own_row_renderer_agrees_with_text_for_every_temporal_family`
//!    exercises the actual replacement renderer
//!    (`staging::apply::row_as_text_jsonb_sql`) against `::text`, and
//!    `a_timestamp_group_key_seeded_by_backfill_and_by_live_read_is_one_group_not_two`
//!    reproduces #113's review finding end-to-end through a real drain —
//!    one Postgres `GROUP BY` group, one target row, seeded partly through
//!    an image-bearing change and partly through a bare, image-less
//!    recompute trigger (the shape that used to force
//!    `staging::apply::read_live_rows_batch`'s now-fixed live refetch).
//!
//! Per ADR-0013 every comparison is against independently-authored SQL —
//! plain `pg_typeof`, plain `select ... group by ...`, plain `::text` —
//! never against `defs::oracle::recompute`, which would be the engine's own
//! renderer grading the engine's own evaluator.
//!
//! Harness conventions follow `defs_floats.rs`.

use std::cmp::Ordering;
use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::eval::{RegexCache, Row, Value, evaluate_aggregate};
use trellis::defs::pg_type::PgType;
use trellis::defs::{create_relationship, parse, validate};
use trellis::integer::IntWidth;
use trellis::temporal::{self, Interval};

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

/// The source table the validator-level tests below install against: one
/// column per temporal family, plus a `bigint` key and a `numeric` measure
/// so a `GROUP BY` definition always has something to aggregate.
const SOURCE_DDL: &str = "create table s ( \
     id bigint primary key, \
     d date, \
     ts timestamp, \
     tstz timestamptz, \
     tm time, \
     tmtz timetz, \
     iv interval, \
     n numeric \
   ); \
   alter table s replica identity full";

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("d".to_string(), ValueType::Other(PgType::Date)),
        ("ts".to_string(), ValueType::Other(PgType::Timestamp)),
        ("tstz".to_string(), ValueType::Other(PgType::TimestampTz)),
        ("tm".to_string(), ValueType::Other(PgType::Time)),
        ("tmtz".to_string(), ValueType::Other(PgType::TimeTz)),
        ("iv".to_string(), ValueType::Other(PgType::Interval)),
        ("n".to_string(), ValueType::Numeric),
    ])
}

/// A raw connection carrying the same output GUCs
/// `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` pins on every engine connection.
///
/// Spelled out here rather than read from that constant on purpose: these
/// tests assert that Trellis's canonical forms *are* what a server so
/// configured emits, so hardcoding the settings makes the test fail if the
/// constant drifts away from them, instead of silently following it.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; \
             set datestyle to 'ISO, YMD'; set intervalstyle to 'postgres'; \
             set timezone to 'UTC'"
        ))
        .await
        .expect("session bootstrap");
    client
}

/// Postgres's own type for `sql_expr`, normalized through `format_type` so
/// it reads in the SQL-standard spelling rather than `pg_typeof`'s internal
/// `timestamptz`/`timetz` abbreviations.
async fn postgres_type_of(client: &Client, sql_expr: &str) -> String {
    client
        .query_one(
            &format!("select format_type(pg_typeof({sql_expr}), null)"),
            &[],
        )
        .await
        .unwrap_or_else(|e| panic!("pg_typeof({sql_expr}): {e}"))
        .get(0)
}

/// `select (<literal>::<pg_type>)::text` — the server's own canonical
/// rendering of a value.
async fn render(client: &Client, pg_type: &str, literal: &str) -> String {
    client
        .query_one(&format!("select ('{literal}'::{pg_type})::text"), &[])
        .await
        .unwrap_or_else(|e| panic!("render {literal}::{pg_type}: {e}"))
        .get(0)
}

/// Every family, paired with the SQL type name and a spread of values
/// chosen to hit the edges each one's rendering has: era boundaries and
/// infinities for `date`/`timestamp`, the legal end-of-day for `time`,
/// non-integral and second-resolution offsets for `timetz`, and — for
/// `interval` — several pairs that are `=` but render differently.
const GRID: &[(PgType, &str, &[&str])] = &[
    (
        PgType::Date,
        "date",
        &[
            "-infinity",
            "4713-01-01 BC",
            "0001-01-01 BC",
            "0001-01-01",
            "1999-12-31",
            "2000-01-01",
            "2024-02-29",
            "2024-03-01",
            "5874897-12-31",
            "infinity",
        ],
    ),
    (
        PgType::Timestamp,
        "timestamp",
        &[
            "-infinity",
            "4714-11-24 00:00:00 BC",
            "0001-01-01 00:00:00",
            "2024-01-01 00:00:00",
            "2024-01-01 00:00:00.000001",
            "2024-01-01 00:00:00.09",
            "2024-01-01 00:00:00.1",
            "2024-01-01 12:34:56.789012",
            "294276-12-31 23:59:59.999999",
            "infinity",
        ],
    ),
    (
        PgType::Time,
        "time",
        &[
            "00:00:00",
            "00:00:00.000001",
            "01:02:03",
            "12:34:56.09",
            "12:34:56.1",
            "23:59:59.999999",
            "24:00:00",
        ],
    ),
    (
        PgType::TimeTz,
        "timetz",
        &[
            "00:00:00+00",
            "11:00:00+00",
            // Same GMT-equivalent instant as `12:00:00+00`, different zone —
            // Postgres orders them apart, and this grid proves it.
            "17:30:00+05:30",
            "12:00:00+00",
            "12:00:00+05:30:15",
            "12:00:00-12",
            "12:00:00+14",
            "23:59:59.999999+00",
            "24:00:00+00",
        ],
    ),
    (
        PgType::TimestampTz,
        "timestamptz",
        &[
            "-infinity",
            "2024-01-01 00:00:00+00",
            "2024-01-01 07:00:00-05",
            "2024-01-01 12:00:00+00",
            "2024-06-15 12:00:00+00",
            "infinity",
        ],
    ),
    (
        PgType::Interval,
        "interval",
        &[
            "-1 day",
            "-01:00:00",
            "00:00:00",
            "00:00:00.000001",
            "2 hours",
            "1 day",
            "24 hours",
            "25 hours",
            "1 mon",
            "30 days",
            "1 year 2 mons 3 days 04:05:06",
        ],
    ),
];

// ---------------------------------------------------------------------
// 1. Text stability — the question the key roles actually turn on
// ---------------------------------------------------------------------

/// For each family, ask the server whether its `::text` rendering is a
/// bijection on its values, and assert `temporal::is_text_stable` agrees.
///
/// The measurement is deliberately the one the engine's key path performs:
/// `count(distinct v)` is how many groups *Postgres* makes, and
/// `count(distinct v::text)` is how many the engine's `::text` matching
/// would make. Equal counts mean a text-keyed `GROUP BY`/join produces
/// exactly Postgres's grouping; unequal means it splits or merges one.
///
/// `interval`'s grid contains `'1 day'`/`'24 hours'` and
/// `'1 mon'`/`'30 days'`, so it is expected to come back with *more* text
/// groups than value groups — the same defect `-0`/`0` is for floats, and
/// the reason `interval` is on neither allowlist.
#[tokio::test]
async fn the_text_stability_verdict_is_the_server_s_not_this_crate_s() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for (pg_type, sql_name, values) in GRID {
        let rows = values
            .iter()
            .map(|v| format!("('{v}'::{sql_name})"))
            .collect::<Vec<_>>()
            .join(",");
        let row = client
            .query_one(
                &format!(
                    "select count(distinct v)::bigint, count(distinct v::text)::bigint \
                     from (values {rows}) t(v)"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("{sql_name} stability probe: {e}"));
        let (by_value, by_text): (i64, i64) = (row.get(0), row.get(1));

        let bijective = by_value == by_text;

        // Within *one* session, `interval` is the only family whose
        // rendering is not a bijection — and that is a property of the
        // type, which no GUC can change.
        assert_eq!(
            bijective,
            *pg_type != PgType::Interval,
            "{sql_name}: Postgres makes {by_value} value groups and {by_text} text groups"
        );

        // Bijective-under-`::text` is necessary and **not sufficient** —
        // that used to be the whole shape of the finding for `timestamp`/
        // `timestamptz`: a single reader groups them perfectly (this probe,
        // one session throughout, measures exactly that), but a *second*
        // renderer used to disagree. For `timestamp` that second renderer
        // was `to_jsonb` (issue #248, see
        // `to_jsonb_and_text_agree_for_every_admitted_key_type`); for
        // `timestamptz` it was additionally the walsender, whose `TimeZone`
        // Trellis couldn't pin until issue #246
        // (`timestamptz_text_moves_with_the_session_timezone` below still
        // demonstrates the underlying hazard `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`
        // now defends against on both the pool and the walsender). Both
        // fixes landed, which is why both families are admitted today.
        //
        // So admission is the conjunction, and this asserts the conjunction
        // rather than re-listing the outcome.
        assert_eq!(
            temporal::is_text_stable(*pg_type),
            bijective && temporal::is_render_consistent(*pg_type),
            "{sql_name}: is_text_stable must be bijectivity AND renderer agreement \
             (bijective = {bijective})"
        );
    }
}

/// `interval`'s specific defect, spelled out rather than inferred from the
/// count above: two values that are `=` and render differently.
///
/// This is the `-0`/`0` of #112, one family over, and it is why no GUC
/// rescues `interval`'s key roles — `IntervalStyle` changes *which* two
/// spellings these are, never that there are two.
#[tokio::test]
async fn equal_intervals_can_render_differently() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for (a, b) in [("24 hours", "1 day"), ("30 days", "1 mon")] {
        let row = client
            .query_one(
                &format!(
                    "select ('{a}'::interval = '{b}'::interval), \
                            ('{a}'::interval)::text, ('{b}'::interval)::text"
                ),
                &[],
            )
            .await
            .expect("the interval demonstration");
        let (equal, a_text, b_text): (bool, String, String) = (row.get(0), row.get(1), row.get(2));
        assert!(equal, "'{a}' and '{b}' are = in Postgres");
        assert_ne!(
            a_text, b_text,
            "...but render differently, which is what breaks a ::text-matched interval key"
        );
    }
}

/// `timestamptz`'s defect used to be different in kind from `interval`'s:
/// the *same* value renders differently depending on the reading session's
/// `TimeZone`, rather than two different values rendering the same.
///
/// Issue #113 considered pinning `TimeZone = 'UTC'` alongside `DateStyle`
/// to make this go away, and declined at the time — see
/// `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` for why (Trellis used to render
/// `timestamptz` on a walsender whose GUCs it could not set, so the pin
/// would have guaranteed an asymmetry it only risked before). Issue #246
/// closed that gap by pinning the walsender too, so `TimeZone` now *is*
/// pinned (to `'UTC'`) on every connection Trellis opens, and `timestamptz`
/// is a text-stable key type as of this issue. What this test still pins is
/// the underlying, session-scoped fact that made the old decision necessary
/// in the first place — an *unpinned* connection's rendering genuinely does
/// move with `TimeZone`, which is exactly the hazard the pin defends
/// against.
#[tokio::test]
async fn timestamptz_text_moves_with_the_session_timezone() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let mut rendered = Vec::new();
    for zone in ["UTC", "America/New_York", "Asia/Kathmandu"] {
        client
            .batch_execute(&format!("set timezone to '{zone}'"))
            .await
            .expect("set timezone");
        rendered.push(render(&client, "timestamptz", "2024-01-01 12:00:00+00").await);
    }
    assert_eq!(
        rendered.len(),
        3,
        "three readings of one instant were collected"
    );
    assert!(
        rendered[0] != rendered[1] && rendered[1] != rendered[2],
        "one timestamptz value rendered as {rendered:?} under three session TimeZones"
    );

    // ...while `date`/`timestamp`/`time`/`timetz` do not move at all, which
    // is the control that makes the claim specific to `timestamptz`.
    for (sql_name, literal) in [
        ("date", "2024-01-01"),
        ("timestamp", "2024-01-01 12:00:00"),
        ("time", "12:00:00"),
        ("timetz", "12:00:00+05:30"),
    ] {
        let mut seen = Vec::new();
        for zone in ["UTC", "America/New_York", "Asia/Kathmandu"] {
            client
                .batch_execute(&format!("set timezone to '{zone}'"))
                .await
                .expect("set timezone");
            seen.push(render(&client, sql_name, literal).await);
        }
        assert!(
            seen.iter().all(|r| *r == seen[0]),
            "{sql_name} must not move with TimeZone, got {seen:?}"
        );
    }
}

/// The same control for `DateStyle`: `time_out`/`timetz_out` are
/// `IMMUTABLE` and read no GUC, while `date_out`/`timestamp_out` are
/// `STABLE` and read `DateStyle` — which is exactly why the constant pins
/// `ISO` and why `TIME`/`TIMETZ` (and not `TIMESTAMPTZ`) earned
/// typed-literal rows.
#[tokio::test]
async fn datestyle_moves_date_and_timestamp_but_not_time() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // The catalog's own verdict, which is what the typed-literal allowlist
    // cites: only `time_out`/`timetz_out` are IMMUTABLE in this block.
    for (proname, expected) in [
        ("time_out", "i"),
        ("timetz_out", "i"),
        ("date_out", "s"),
        ("timestamp_out", "s"),
        ("timestamptz_out", "s"),
        ("interval_out", "s"),
    ] {
        let volatility: String = client
            .query_one(
                "select provolatile::text from pg_proc where proname = $1",
                &[&proname],
            )
            .await
            .unwrap_or_else(|e| panic!("provolatile({proname}): {e}"))
            .get(0);
        assert_eq!(volatility, expected, "pg_proc.provolatile for {proname}");
    }

    for style in ["ISO, YMD", "SQL, DMY", "Postgres, DMY", "German, DMY"] {
        client
            .batch_execute(&format!("set datestyle to '{style}'"))
            .await
            .expect("set datestyle");
        assert_eq!(
            render(&client, "time", "12:34:56.5").await,
            "12:34:56.5",
            "time_out must not move with DateStyle ({style})"
        );
        assert_eq!(
            render(&client, "timetz", "12:00:00+05:30").await,
            "12:00:00+05:30",
            "timetz_out must not move with DateStyle ({style})"
        );
    }

    client
        .batch_execute("set datestyle to 'SQL, MDY'")
        .await
        .expect("set datestyle");
    assert_ne!(
        render(&client, "date", "2024-01-02").await,
        "2024-01-02",
        "date_out *does* move with DateStyle, which is why the constant pins ISO"
    );
}

// ---------------------------------------------------------------------
// 2. Comparison order
// ---------------------------------------------------------------------

/// `temporal::compare` must reproduce each family's own comparison
/// operator, over the whole [`GRID`] — every ordered pair, in both
/// directions.
///
/// The server's verdict is read as two booleans (`a < b`, `a = b`) rather
/// than a single `cmp` function, so this compares against plain SQL
/// operators rather than against a `*_cmp` support function the engine
/// could have been written from.
#[tokio::test]
async fn temporal_compare_matches_each_family_s_own_operators() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for (pg_type, sql_name, values) in GRID {
        // Compare the *canonical renderings*, which is what the engine
        // actually holds — not the input literals, which may be
        // non-canonical (`'24 hours'`).
        let mut canonical = Vec::new();
        for value in *values {
            canonical.push(render(&client, sql_name, value).await);
        }

        for a in &canonical {
            for b in &canonical {
                let row = client
                    .query_one(
                        &format!(
                            "select ('{a}'::{sql_name} < '{b}'::{sql_name}), \
                                    ('{a}'::{sql_name} = '{b}'::{sql_name})"
                        ),
                        &[],
                    )
                    .await
                    .unwrap_or_else(|e| panic!("{sql_name}: {a} vs {b}: {e}"));
                let (less, equal): (bool, bool) = (row.get(0), row.get(1));
                let expected = if equal {
                    Ordering::Equal
                } else if less {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
                let actual = temporal::compare(*pg_type, a, b).unwrap_or_else(|| {
                    panic!("temporal::compare must handle canonical {sql_name} {a:?}/{b:?}")
                });
                assert_eq!(
                    actual, expected,
                    "{sql_name}: {a} vs {b} — Postgres says {expected:?}"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------
// 3. `interval` arithmetic and rendering fidelity
// ---------------------------------------------------------------------

/// `Interval::parse`/`render` must round-trip every canonical
/// `interval_out` rendering the server produces, and `Interval::checked_add`
/// must agree with `interval_pl` byte-for-byte.
///
/// This is the issue's "match Postgres `interval` canonicalization exactly"
/// requirement, and it is checked the only way that means anything: by
/// comparing against the server's own output for the same operands, not
/// against a second copy of the same rules.
#[tokio::test]
async fn interval_arithmetic_and_rendering_match_the_server() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // A spread that exercises every branch of `EncodeInterval`'s postgres
    // style: pluralisation, the `is_before` `+` prefix, the omitted
    // all-zero time field, fractional trimming, and the year/month split.
    const OPERANDS: &[&str] = &[
        "0",
        "1 year",
        "2 years",
        "-13 mons",
        "1 day",
        "-1 day",
        "24 hours",
        "-1 day 1 hour",
        "1 day -1 hour",
        "1 mon -1 day 1 hour",
        "0.5 secs",
        "-0.000001 secs",
        "100:00:00",
        "1 year 2 mons 3 days 04:05:06",
    ];

    for literal in OPERANDS {
        let server = render(&client, "interval", literal).await;
        let parsed = Interval::parse(&server)
            .unwrap_or_else(|| panic!("Interval::parse must accept interval_out's {server:?}"));
        assert_eq!(
            parsed.render(),
            server,
            "Interval::render must reproduce interval_out for {literal:?}"
        );
    }

    for a in OPERANDS {
        for b in OPERANDS {
            let server: String = client
                .query_one(
                    &format!("select ('{a}'::interval + '{b}'::interval)::text"),
                    &[],
                )
                .await
                .unwrap_or_else(|e| panic!("{a} + {b}: {e}"))
                .get(0);
            let lhs = Interval::parse(&render(&client, "interval", a).await).expect("lhs parses");
            let rhs = Interval::parse(&render(&client, "interval", b).await).expect("rhs parses");
            let ours = lhs
                .checked_add(rhs)
                .unwrap_or_else(|e| panic!("{a} + {b} must not overflow: {e}"))
                .render();
            assert_eq!(ours, server, "{a} + {b}");
        }
    }
}

/// Postgres does not justify an interval sum, and this is what "1 day =
/// 24h" does *and does not* mean: the two are `=` for **comparison**, but
/// addition keeps the fields apart, so `'1 mon' + '30 days'` is `1 mon 30
/// days` and not `2 mons` — even though those two are themselves `=`.
///
/// Getting this backwards is the fidelity risk the issue names, so it gets
/// its own test rather than riding on the grid above.
#[tokio::test]
async fn interval_addition_does_not_justify() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let row = client
        .query_one(
            "select ('1 mon'::interval + '30 days'::interval)::text, \
                    (('1 mon'::interval + '30 days'::interval) = '2 mons'::interval)",
            &[],
        )
        .await
        .expect("the justification demonstration");
    let (text, equals_two_months): (String, bool) = (row.get(0), row.get(1));
    assert_eq!(text, "1 mon 30 days", "Postgres does not justify a sum");
    assert!(
        equals_two_months,
        "...even though the unjustified result is = to the justified one"
    );

    let ours = Interval::parse("1 mon")
        .unwrap()
        .checked_add(Interval::parse("30 days").unwrap())
        .unwrap();
    assert_eq!(ours.render(), text);
}

/// `interval` addition overflows rather than wrapping, matching
/// `select '2147483647 months'::interval + '1 month'::interval`.
#[tokio::test]
async fn interval_overflow_agrees_with_the_server() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let server_failed = client
        .query_one(
            "select ('2147483647 months'::interval + '1 month'::interval)::text",
            &[],
        )
        .await
        .is_err();
    let ours_failed = Interval {
        months: i32::MAX,
        days: 0,
        micros: 0,
    }
    .checked_add(Interval {
        months: 1,
        days: 0,
        micros: 0,
    })
    .is_err();
    assert!(server_failed, "Postgres raises `interval out of range`");
    assert_eq!(ours_failed, server_failed);
}

// ---------------------------------------------------------------------
// 4. Aggregates
// ---------------------------------------------------------------------

/// `min`/`max` keep their argument's own type for every temporal family,
/// and `sum(interval)` is `interval` — read off `pg_typeof`, not from the
/// docs, exactly as #111/#112 did for their families.
#[tokio::test]
async fn temporal_aggregate_result_types_match_pg_typeof() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client.batch_execute(SOURCE_DDL).await.expect("source ddl");
    client
        .execute(
            "insert into s (id, d, ts, tstz, tm, tmtz, iv, n) values \
             (1, '2024-01-01', '2024-01-01 00:00:00', '2024-01-01 00:00:00+00', \
              '12:00:00', '12:00:00+00', '1 day', 1)",
            &[],
        )
        .await
        .expect("seed");

    for (column, sql_name) in [
        ("d", "date"),
        ("tm", "time without time zone"),
        ("tmtz", "time with time zone"),
        ("ts", "timestamp without time zone"),
        ("tstz", "timestamp with time zone"),
    ] {
        for aggregate in ["min", "max"] {
            let pg =
                postgres_type_of(&client, &format!("(select {aggregate}({column}) from s)")).await;
            assert_eq!(pg, sql_name, "{aggregate}({column}) in Postgres");

            let ours = trellis::defs::registry::aggregate_result_type(
                &aggregate.to_uppercase(),
                source_columns()[column],
            )
            .unwrap_or_else(|| panic!("{aggregate}({column}) must resolve"));
            assert_eq!(
                ours,
                source_columns()[column],
                "{aggregate}({column}) keeps its argument's type, like Postgres"
            );
        }
    }

    let pg = postgres_type_of(&client, "(select sum(iv) from s)").await;
    assert_eq!(pg, "interval");
    assert_eq!(
        trellis::defs::registry::aggregate_result_type("SUM", ValueType::Other(PgType::Interval)),
        Some(ValueType::Other(PgType::Interval))
    );

    // Postgres has no `sum`/`avg` over the other five, and no `avg` over
    // `interval` either — so neither does the registry.
    for column in ["d", "ts", "tstz", "tm", "tmtz"] {
        assert_eq!(
            trellis::defs::registry::aggregate_result_type("SUM", source_columns()[column]),
            None,
            "there is no sum({column}) in Postgres"
        );
    }
    assert_eq!(
        trellis::defs::registry::aggregate_result_type("AVG", ValueType::Other(PgType::Interval)),
        None,
        "there is no avg(interval) in Postgres"
    );
}

/// **Why `MIN`/`MAX(interval)` is refused**, demonstrated from the server.
///
/// `max(v)` over the same three rows returns different *text* depending on
/// the scan order, because `interval_larger` is a left fold
/// (`cmp(arg1, arg2) < 0 ? arg1 : arg2`) and `'1 day'`/`'24 hours'` tie.
/// Postgres's own answer is therefore not a function of its input multiset,
/// which means ADR-0013's byte-exact recompute cross-check can never settle
/// it — whatever Trellis computes, a recompute is free to disagree. So the
/// aggregate is refused at define time rather than shipped with a
/// known-flaky self-check.
///
/// The control is `date`, whose ties are byte-identical by construction and
/// which is therefore admitted.
#[tokio::test]
async fn max_interval_is_not_a_function_of_its_input_which_is_why_it_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table iv_rows (id int primary key, v interval, d date); \
             insert into iv_rows values \
               (1, '1 day', '2024-01-01'), \
               (2, '24 hours', '2024-01-01'), \
               (3, '2 hours', '2023-06-01')",
        )
        .await
        .expect("seed the tie");

    let mut interval_answers = Vec::new();
    let mut date_answers = Vec::new();
    for direction in ["asc", "desc"] {
        let row = client
            .query_one(
                &format!(
                    "select max(v)::text, max(d)::text \
                     from (select v, d from iv_rows order by id {direction}) s"
                ),
                &[],
            )
            .await
            .expect("scan-order probe");
        interval_answers.push(row.get::<_, String>(0));
        date_answers.push(row.get::<_, String>(1));
    }

    assert_ne!(
        interval_answers[0], interval_answers[1],
        "Postgres's own max(interval) returned {interval_answers:?} for two scan orders — \
         this is the fact the refusal rests on"
    );
    assert!(
        !temporal::supports_min_max(PgType::Interval),
        "so MIN/MAX(interval) must be refused"
    );

    assert_eq!(
        date_answers[0], date_answers[1],
        "the control: max(date) is scan-order independent"
    );
    assert!(temporal::supports_min_max(PgType::Date));
}

/// The evaluator's `MIN`/`MAX` fold over each admitted family must produce
/// byte-identical text to a server-side `min()`/`max()` over the same rows
/// (ADR-0013: independently-authored SQL, not `defs::oracle`).
#[tokio::test]
async fn the_min_max_fold_matches_a_server_side_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for (pg_type, sql_name, values) in GRID {
        if !temporal::supports_min_max(*pg_type) {
            continue;
        }
        let rows = values
            .iter()
            .map(|v| format!("('{v}'::{sql_name})"))
            .collect::<Vec<_>>()
            .join(",");
        let row = client
            .query_one(
                &format!("select min(v)::text, max(v)::text from (values {rows}) t(v)"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("{sql_name} min/max: {e}"));
        let (server_min, server_max): (String, String) = (row.get(0), row.get(1));

        // The engine holds canonical renderings, so feed it those.
        let mut canonical = Vec::new();
        for value in *values {
            canonical.push(render(&client, sql_name, value).await);
        }

        for (aggregate, expected) in [("MIN", &server_min), ("MAX", &server_max)] {
            let def = parse(&format!(
                "TRANSFORM t FROM s GROUP BY id SELECT id AS k, {aggregate}(col) AS out"
            ))
            .expect("parses");
            let columns = HashMap::from([
                ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
                ("col".to_string(), ValueType::Other(*pg_type)),
            ]);
            validate(&def, &columns, &HashMap::new())
                .unwrap_or_else(|e| panic!("{aggregate}({sql_name}) must validate: {e}"));

            let group: Vec<Row> = canonical
                .iter()
                .map(|text| {
                    Row::from([
                        ("id".to_string(), Some("1".to_string())),
                        ("col".to_string(), Some(text.clone())),
                    ])
                })
                .collect();
            let out = evaluate_aggregate(&def, &group, &columns, &mut RegexCache::default())
                .unwrap_or_else(|e| panic!("{aggregate}({sql_name}) fold: {e}"))
                .remove("out")
                .expect("the aggregate field")
                .expect("a non-empty group folds to a value");
            assert_eq!(
                out,
                Value::Other(*pg_type, expected.clone()),
                "{aggregate}({sql_name}) must match a server-side aggregate"
            );
        }
    }
}

/// The `SUM(interval)` fold must likewise match a server-side `sum()`,
/// including over a group whose members are `=` but spelled differently —
/// the case that makes `MIN`/`MAX` unusable but leaves `SUM` perfectly
/// well-defined, because a sum reads all three fields of every input rather
/// than picking one input to return.
#[tokio::test]
async fn the_sum_interval_fold_matches_a_server_side_sum() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    const GROUPS: &[&[&str]] = &[
        &["1 day", "24 hours", "2 hours"],
        &["1 mon", "30 days", "-1 day"],
        &["0.5 secs", "-0.000001 secs", "100:00:00"],
        &["1 year 2 mons 3 days 04:05:06", "-1 day 1 hour"],
    ];

    for group in GROUPS {
        let rows = group
            .iter()
            .map(|v| format!("('{v}'::interval)"))
            .collect::<Vec<_>>()
            .join(",");
        let server: String = client
            .query_one(
                &format!("select sum(v)::text from (values {rows}) t(v)"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("sum over {group:?}: {e}"))
            .get(0);

        let def = parse("TRANSFORM t FROM s GROUP BY id SELECT id AS k, SUM(col) AS out")
            .expect("parses");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
            ("col".to_string(), ValueType::Other(PgType::Interval)),
        ]);
        validate(&def, &columns, &HashMap::new()).expect("SUM(interval) must validate");

        let mut rows_in = Vec::new();
        for value in *group {
            let canonical = render(&client, "interval", value).await;
            rows_in.push(Row::from([
                ("id".to_string(), Some("1".to_string())),
                ("col".to_string(), Some(canonical)),
            ]));
        }
        let out = evaluate_aggregate(&def, &rows_in, &columns, &mut RegexCache::default())
            .expect("fold")
            .remove("out")
            .expect("the aggregate field")
            .expect("a non-empty group folds to a value");
        assert_eq!(
            out,
            Value::Other(PgType::Interval, server.clone()),
            "SUM over {group:?}"
        );
    }
}

/// Interval addition really is exact, commutative and associative *on
/// finite, in-range values* — unlike float addition, which fails all three.
/// And `SUM(interval)` is still **recompute-only**, because that monoid is
/// partial and a delta cannot represent the gaps.
///
/// Both halves are pinned here against the server, because the first half
/// is what makes the second half surprising: this is not "interval is like
/// float", it is "interval is exact but its arithmetic can *raise*, in
/// ways that depend on accumulation order a delta does not control".
#[tokio::test]
async fn sum_interval_is_exact_on_finite_values_but_still_recompute_only() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let row = client
        .query_one(
            "select (((('1 mon'::interval + '30 days') + '2 hours') - '30 days') \
                     - '2 hours')::text, \
                    (select sum(v)::text from (values ('1 day'::interval),('24 hours'), \
                                                      ('2 hours')) t(v)), \
                    (select sum(v)::text from (values ('2 hours'::interval),('24 hours'), \
                                                      ('1 day')) t(v))",
            &[],
        )
        .await
        .expect("the invertibility demonstration");
    let (round_tripped, forward, reversed): (String, String, String) =
        (row.get(0), row.get(1), row.get(2));

    assert_eq!(
        round_tripped, "1 mon",
        "adding then subtracting the same intervals returns exactly the original"
    );
    assert_eq!(
        forward, reversed,
        "sum(interval) is order-independent, unlike sum(float)"
    );

    // ...and yet `SUM(interval)` is still **recompute-only**, because the
    // monoid is partial. Both gaps, from the same server:
    //
    //   select sum(v) from (values ('infinity'::interval),('-infinity')) t(v)
    //     -> ERROR: interval out of range
    //   select sum(v) from (select v from ovf order by id)       -> ERROR
    //   select sum(v) from (select v from ovf order by id desc)  -> ok
    //     (ovf = {'2147483647 days', '1 days', '-1 days'})
    //
    // A delta accumulates in arrival order and computes a deletion as
    // `+ (-x)`, so it can raise on a group whose true sum is perfectly
    // well-defined — and since the delta replays identically on retry,
    // that is a drain that never progresses, not a quarantine. A
    // recompute lets Postgres fold the group in one pass and raises
    // exactly when a server-side `sum()` would.
    let mixed = client
        .query_one(
            "select sum(v)::text from (values ('infinity'::interval),('-infinity')) t(v)",
            &[],
        )
        .await;
    assert!(
        mixed.is_err(),
        "infinity + -infinity must be an error, which is the case a delta cannot survive"
    );

    let verdict = trellis::defs::invertibility::classify(
        "SUM",
        trellis::defs::invertibility::AggregateArg::Column(ValueType::Other(PgType::Interval)),
    )
    .expect("SUM(interval) must classify");
    assert!(
        !verdict.is_invertible(),
        "SUM(interval) belongs on the recompute path, alongside float SUM/AVG"
    );
}

/// **The guard for the #113 review's finding.** Every type Trellis admits
/// as a key must render *identically* under both of the engine's two
/// renderers — the per-column `<col>::text` most paths use, and the
/// whole-row `to_jsonb(t.*)`/`jsonb_each_text` the live-row reads use
/// (`staging::apply`'s `read_live_rows_batch`, `fetch_to_side_rows`,
/// `fetch_relationship_projection_rows`; `staging::quarantine`'s sweep).
///
/// A type that disagrees is one value with two spellings, and a key seeded
/// once through a backfill and once through CDC-then-live-read lands as two
/// target rows for one Postgres group. `timestamp`/`timestamptz` disagree
/// under raw `to_jsonb(t.*)` — it writes an ISO-8601 `T` — which is why
/// **that specific renderer** is never used for a row read any more
/// (`staging::apply::row_as_text_jsonb_sql` replaced it, issue #248); both
/// are admitted key types today despite this raw disagreement persisting,
/// which this test also pins so a future regression back to raw
/// `to_jsonb(t.*)` fails loudly here instead of shipping silently.
///
/// This sweeps the *whole* allowlist rather than the temporal families, so
/// it also guards the types #111/#112 and earlier issues admitted, and will
/// fail for any family a future issue adds without checking.
#[tokio::test]
async fn to_jsonb_and_text_agree_for_every_admitted_key_type() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // Column name -> (declared type, a literal). Every currently-admitted
    // key type, plus the four refused temporal families as controls.
    const COLUMNS: &[(&str, &str, &str, bool)] = &[
        ("c_int2", "smallint", "42", true),
        ("c_int4", "integer", "42", true),
        ("c_int8", "bigint", "42", true),
        ("c_oid", "oid", "42", true),
        (
            "c_uuid",
            "uuid",
            "00000000-0000-0000-0000-000000000001",
            true,
        ),
        ("c_text", "text", "hello", true),
        ("c_varchar", "character varying(16)", "hello", true),
        ("c_date", "date", "2024-06-15", true),
        ("c_time", "time", "12:34:56", true),
        ("c_timetz", "timetz", "12:34:56+00", true),
        // Controls: both admitted key types (`timestamp` since #248,
        // `timestamptz` since #246) despite raw `to_jsonb(t.*)` disagreeing
        // with `::text` for both — the reason that disagreement doesn't
        // block admission is that nothing inside Trellis calls raw
        // `to_jsonb(t.*)` any more (`staging::apply::row_as_text_jsonb_sql`
        // replaced it; see `the_engine_s_own_row_renderer_agrees_with_text_for_every_temporal_family`
        // below).
        ("c_ts", "timestamp", "2024-06-15 12:34:56", false),
        ("c_tstz", "timestamptz", "2024-06-15 12:34:56+00", false),
        // `interval` agrees under both renderers — its refusal is about
        // equal values rendering differently, a different defect entirely.
        ("c_iv", "interval", "1 day 2 hours", true),
    ];

    let ddl = COLUMNS
        .iter()
        .map(|(name, pg_type, _, _)| format!("{name} {pg_type}"))
        .collect::<Vec<_>>()
        .join(", ");
    let values = COLUMNS
        .iter()
        .map(|(_, _, literal, _)| format!("'{literal}'"))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .batch_execute(&format!("create table renderers ({ddl})"))
        .await
        .expect("create sweep table");
    client
        .batch_execute(&format!("insert into renderers values ({values})"))
        .await
        .expect("seed sweep row");

    let via_jsonb: HashMap<String, String> = client
        .query(
            "select e.key, e.value from renderers              cross join lateral jsonb_each_text(to_jsonb(renderers.*)) e",
            &[],
        )
        .await
        .expect("to_jsonb sweep")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

    for (name, pg_type, _, expect_agreement) in COLUMNS {
        let via_text: String = client
            .query_one(&format!("select {name}::text from renderers"), &[])
            .await
            .unwrap_or_else(|e| panic!("::text for {name}: {e}"))
            .get(0);
        let jsonb = &via_jsonb[*name];
        assert_eq!(
            via_text == *jsonb,
            *expect_agreement,
            "{pg_type}: ::text = {via_text:?}, to_jsonb = {jsonb:?}"
        );
    }

    // Raw `to_jsonb(t.*)` still disagrees with `::text` for `timestamp` and
    // `timestamptz` (see `c_ts`/`c_tstz` above, `expect_agreement = false`)
    // — that is a fact about bare Postgres, unaffected by issue #248, and
    // it will stay true forever. What #248 changed is that **nothing inside
    // Trellis calls raw `to_jsonb(t.*)` for a row read any more** (see
    // `the_engine_s_own_row_renderer_agrees_with_text_for_every_temporal_family`,
    // below, which cross-checks the actual renderer
    // `staging::apply::row_as_text_jsonb_sql` now uses), so both are
    // admitted here despite this raw-`to_jsonb` disagreement persisting —
    // `timestamptz` needed issue #246 on top of #248 (the pool-vs-walsender
    // `TimeZone` gap, independent of `to_jsonb` vs. `::text`) before it
    // could join. `interval` is admitted here too (raw `to_jsonb` agrees
    // with `::text` for it) but still refused every key role, because it
    // fails the *other* half of `is_text_stable` — equal values render
    // differently (`'1 day'` vs `'24 hours'`), which no renderer
    // reconciliation can fix.
    assert!(temporal::is_render_consistent(PgType::Timestamp));
    assert!(temporal::is_text_stable(PgType::Timestamp));
    assert!(temporal::supports_min_max(PgType::Timestamp));

    assert!(temporal::is_render_consistent(PgType::TimestampTz));
    assert!(temporal::is_text_stable(PgType::TimestampTz));
    assert!(temporal::supports_min_max(PgType::TimestampTz));

    assert!(temporal::is_render_consistent(PgType::Interval));
    assert!(!temporal::is_text_stable(PgType::Interval));
    assert!(!temporal::supports_min_max(PgType::Interval));
}

/// Issue #248's actual fix, exercised directly against a live server: the
/// renderer Trellis now uses in place of `to_jsonb(t.*)` —
/// `staging::apply::live_row_columns` (the live column list) plus
/// `staging::apply::row_as_text_jsonb_sql` (the explicit per-column
/// `jsonb_build_object('<col>', <col>::text, ...)` built from it) — agrees
/// with `::text` for **every** temporal family, `timestamp` and
/// `timestamptz` included.
///
/// This is the direct counterpart to
/// `to_jsonb_and_text_agree_for_every_admitted_key_type` above, but it
/// exercises the actual SQL Trellis's read paths now build, rather than bare
/// `to_jsonb(t.*)`. It deliberately covers `timestamptz` too, even though
/// `timestamptz` still holds no key/MIN-MAX role: that refusal is issue
/// #246 (a *different* renderer pair — the pool vs. the walsender — which
/// this fix does not and cannot touch), not a residual `to_jsonb`-vs-`::text`
/// disagreement. Proving the renderer itself is fully reconciled here is
/// what makes it legible, from the test suite alone, that `timestamptz`'s
/// continued refusal in `trellis::temporal` is a deliberate, independent
/// deferral rather than an oversight.
#[tokio::test]
async fn the_engine_s_own_row_renderer_agrees_with_text_for_every_temporal_family() {
    use trellis::staging::apply;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    const COLUMNS: &[(&str, &str, &str)] = &[
        ("c_date", "date", "2024-06-15"),
        ("c_time", "time", "12:34:56"),
        ("c_timetz", "timetz", "12:34:56+00"),
        ("c_ts", "timestamp", "2024-06-15 12:34:56"),
        ("c_tstz", "timestamptz", "2024-06-15 12:34:56+00"),
        ("c_iv", "interval", "1 day 2 hours"),
    ];

    let ddl = COLUMNS
        .iter()
        .map(|(name, pg_type, _)| format!("{name} {pg_type}"))
        .collect::<Vec<_>>()
        .join(", ");
    let values = COLUMNS
        .iter()
        .map(|(_, _, literal)| format!("'{literal}'"))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .batch_execute(&format!("create table engine_renderer_sweep ({ddl})"))
        .await
        .expect("create sweep table");
    client
        .batch_execute(&format!(
            "insert into engine_renderer_sweep values ({values})"
        ))
        .await
        .expect("seed sweep row");

    // The exact call shape `read_live_rows_batch`/`fetch_to_side_rows`/etc.
    // now use: introspect the live column list, then build the explicit
    // `jsonb_build_object` from it.
    let row_columns = apply::live_row_columns(&client, "engine_renderer_sweep")
        .await
        .expect("introspect columns");
    let doc_expr = apply::row_as_text_jsonb_sql("t", &row_columns);
    let via_engine: HashMap<String, String> = client
        .query(
            &format!(
                "select e.key, e.value from engine_renderer_sweep t \
                 cross join lateral jsonb_each_text({doc_expr}) e"
            ),
            &[],
        )
        .await
        .expect("engine renderer sweep")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();

    for (name, pg_type, _) in COLUMNS {
        let via_text: String = client
            .query_one(
                &format!("select {name}::text from engine_renderer_sweep"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("::text for {name}: {e}"))
            .get(0);
        assert_eq!(
            via_text, via_engine[*name],
            "{pg_type}: the engine's own row renderer must agree with ::text (issue #248)"
        );
    }
}

// ---------------------------------------------------------------------
// 5. Key roles
// ---------------------------------------------------------------------

/// `date`/`timestamp`/`time`/`timetz`/`timestamptz` are **accepted** as
/// relationship join keys (and therefore as 1-1 primary keys — one
/// allowlist, `catalog::TEXT_STABLE_JOIN_KEY_TYPES`, gates both), while
/// `interval` alone is refused.
///
/// This is the headline change of #113 (`date`/`time`/`timetz`) plus #248
/// (`timestamp`, once its `to_jsonb`-vs-`::text` divergence was fixed) plus
/// #246 (`timestamptz`, once the walsender could be pinned to the same
/// `TimeZone` as the pool), and the split is the point: the matrix had all
/// six marked `🎯 typed index`, and five of them never needed the index at
/// all.
#[tokio::test]
async fn temporal_join_keys_are_admitted_per_family() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table p_date (k date primary key); \
             create table p_ts (k timestamp primary key); \
             create table p_tm (k time primary key); \
             create table p_tmtz (k timetz primary key); \
             create table p_tstz (k timestamptz primary key); \
             create table p_iv (k interval primary key); \
             create table child ( \
               id bigint primary key, d date, ts timestamp, tm time, \
               tmtz timetz, tstz timestamptz, iv interval); \
             alter table child replica identity full; \
             alter table p_date replica identity full; \
             alter table p_ts replica identity full; \
             alter table p_tm replica identity full; \
             alter table p_tmtz replica identity full; \
             alter table p_tstz replica identity full; \
             alter table p_iv replica identity full",
        )
        .await
        .expect("create relationship tables");

    for (name, from_col, to_table) in [
        ("r_date", "d", "p_date"),
        ("r_tm", "tm", "p_tm"),
        ("r_tmtz", "tmtz", "p_tmtz"),
        // `timestamp` used to be refused for the #113 review's reason: it is
        // a bijection under `::text` but `to_jsonb` spelled it with an
        // ISO-8601 `T`, and the engine used both renderers. Issue #248 fixed
        // that (see `to_jsonb_and_text_agree_for_every_admitted_key_type`
        // and `the_engine_s_own_row_renderer_agrees_with_text_for_every_temporal_family`
        // below), so `timestamp` now joins the accepted group.
        ("r_ts", "ts", "p_ts"),
        // `timestamptz` used to be refused for a *second*, independent
        // reason on top of the `to_jsonb` one above: its rendering moved
        // with `TimeZone`, and issue #113 could pin that on the pool but
        // not on the walsender. Issue #246 closed that gap
        // (`pgwire_replication::ReplicationConfig::with_options`), so
        // `timestamptz` joins the accepted group too.
        ("r_tstz", "tstz", "p_tstz"),
    ] {
        create_relationship(
            &db.pool,
            &format!("RELATIONSHIP {name} FROM child.{from_col} TO {to_table}.k"),
        )
        .await
        .unwrap_or_else(|e| panic!("{from_col} must be accepted as a join key: {e}"));
    }

    // `interval` is the one remaining refusal: `'24 hours'` and `'1 day'`
    // are `=` in Postgres but render differently, so no GUC or renderer
    // reconciliation fixes it — see this module's doc comment.
    let (name, from_col, to_table, pg_name) = ("r_iv", "iv", "p_iv", "interval");
    let err = create_relationship(
        &db.pool,
        &format!("RELATIONSHIP {name} FROM child.{from_col} TO {to_table}.k"),
    )
    .await
    .expect_err("an interval join key must be refused");
    assert!(
        err.to_string().contains(pg_name),
        "the rejection must name the offending type ({pg_name}): {err}"
    );
}

/// A `timestamp(3)` column must be recognized as `timestamp without time
/// zone` despite `format_type` rendering its modifier *inside* the name.
///
/// This is the concrete reason `catalog::base_type_name` replaced
/// `split('(')`: `timestamp(3) without time zone` truncates to `timestamp`,
/// which is on no list, so a perfectly good sub-second-precision key would
/// have been silently refused.
#[tokio::test]
async fn a_precision_modified_temporal_key_is_still_recognized() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table probe (k timestamp(3))")
        .await
        .expect("create probe");
    let rendered: String = client
        .query_one(
            "select format_type(atttypid, atttypmod) from pg_attribute \
             where attrelid = 'probe'::regclass and attname = 'k'",
            &[],
        )
        .await
        .expect("format_type probe")
        .get(0);
    assert_eq!(
        rendered, "timestamp(3) without time zone",
        "format_type puts the modifier inside the name, which is the whole hazard"
    );

    // The *admitted* family with the same modifier-inside-the-name shape:
    // `format_type` renders this `time(2) with time zone`, which the old
    // `split('(')` truncated to `time`. (`timestamp(3)` above shows the
    // rendering hazard itself; `timetz` is what actually exercises the fix
    // end-to-end. `timestamp` is an admitted key type since #248, but this
    // test predates that and there's no need to duplicate coverage here —
    // `temporal_join_keys_are_admitted_per_family` already covers plain
    // `timestamp`.)
    client
        .batch_execute(
            "create table p3 (k timetz(2) primary key); \
             create table c3 (id bigint primary key, k timetz(2)); \
             alter table c3 replica identity full; \
             alter table p3 replica identity full",
        )
        .await
        .expect("create modified-precision tables");
    create_relationship(&db.pool, "RELATIONSHIP r3 FROM c3.k TO p3.k")
        .await
        .expect("a timetz(2) join key must be accepted");
}

/// The `GROUP BY` key gate follows the same five-accepted/one-refused
/// split, and the rejection names the column.
#[test]
fn the_group_by_key_gate_follows_the_same_split() {
    for column in ["d", "tm", "tmtz", "ts", "tstz"] {
        let def = parse(&format!(
            "TRANSFORM t FROM s GROUP BY {column} SELECT {column} AS k, SUM(n) AS total"
        ))
        .expect("parses");
        validate(&def, &source_columns(), &HashMap::new())
            .unwrap_or_else(|e| panic!("{column} must be accepted as a GROUP BY key: {e}"));
    }

    {
        let column = "iv";
        let def = parse(&format!(
            "TRANSFORM t FROM s GROUP BY {column} SELECT {column} AS k, SUM(n) AS total"
        ))
        .expect("parses");
        let err = validate(&def, &source_columns(), &HashMap::new())
            .expect_err("an interval GROUP BY key must be refused");
        assert!(
            err.to_string().contains(column),
            "the rejection must name the column: {err}"
        );
    }
}

// ---------------------------------------------------------------------
// 6. Literals
// ---------------------------------------------------------------------

/// `TIME`/`TIMETZ` literals must be canonical, and Postgres must parse the
/// same spelling to the same type.
///
/// `TIMESTAMPTZ` and `INTERVAL` are deliberately absent from the grammar;
/// the rejections below pin that, with the reasons in
/// `defs::typed_literal`'s allowlist doc comment.
#[tokio::test]
async fn time_and_timetz_literals_are_canonical_only() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for good in [
        "TIME '13:45:00'",
        "TIME '13:45:00.5'",
        "TIME '24:00:00'",
        "TIME '00:00:00'",
        "CAST('13:45:00' AS time)",
        "TIMETZ '13:45:00+00'",
        "TIMETZ '13:45:00.5-05:30'",
        "TIMETZ '13:45:00+05:30:15'",
        // A zero *minutes* field with a non-zero seconds field IS canonical
        // — `timetz_out` drops only a trailing all-zero tail, so
        // `'12:00:00+05:00:30'` prints as itself (#113 review).
        "TIMETZ '13:45:00+05:00:30'",
        "TIMETZ '13:45:00+00:00:30'",
        "TIMETZ '13:45:00-00:00:30'",
        "TIMETZ '13:45:00+15:59:59'",
        "CAST('13:45:00+00' AS timetz)",
    ] {
        let def = parse(&format!("TRANSFORM t FROM s SELECT {good} AS x"))
            .unwrap_or_else(|e| panic!("{good} should parse: {e}"));
        validate(&def, &source_columns(), &HashMap::new())
            .unwrap_or_else(|e| panic!("{good} should validate: {e}"));

        let pg = postgres_type_of(&client, good).await;
        assert!(
            pg == "time without time zone" || pg == "time with time zone",
            "{good} types as {pg} in Postgres"
        );

        // And the literal text is exactly what Postgres renders back — the
        // round-trip identity the typed-literal module's doc comment
        // requires.
        let literal = good
            .split_once('\'')
            .and_then(|(_, rest)| rest.rsplit_once('\''))
            .map(|(text, _)| text)
            .expect("every spelling above contains a quoted literal");
        let sql_name = if pg.contains("with time zone") {
            "timetz"
        } else {
            "time"
        };
        assert_eq!(
            render(&client, sql_name, literal).await,
            literal,
            "{good} must round-trip to the text it was written as"
        );
    }

    for bad in [
        // Non-canonical spellings `time_in` accepts but `time_out` never
        // emits.
        "TIME '13:45'",
        "TIME '13:45:00.500'",
        "TIME '1:45:00'",
        "TIME '24:00:01'",
        // Relative spellings — the reason `time_in` is STABLE.
        "TIME 'now'",
        "TIME 'allballs'",
        // A zone abbreviation is a lookup against the server's timezone
        // set, which is what makes `timetz_in` STABLE.
        "TIMETZ '13:45:00 EST'",
        "TIMETZ '13:45:00'",
        // `timetz_out` omits a zero offset field, so a padded one would not
        // round-trip; and the offset is capped at 15:59:59.
        "TIMETZ '13:45:00+00:00'",
        "TIMETZ '13:45:00+05:30:00'",
        "TIMETZ '13:45:00+05:00:00'",
        "TIMETZ '13:45:00+16'",
    ] {
        let parsed = parse(&format!("TRANSFORM t FROM s SELECT {bad} AS x"));
        let rejected = match parsed {
            Err(_) => true,
            Ok(def) => validate(&def, &source_columns(), &HashMap::new()).is_err(),
        };
        assert!(rejected, "{bad} must be rejected");
    }

    // The two temporal families with no literal row at all.
    for absent in ["TIMESTAMPTZ '2024-01-01 00:00:00+00'", "INTERVAL '1 day'"] {
        let parsed = parse(&format!("TRANSFORM t FROM s SELECT {absent} AS x"));
        let rejected = match parsed {
            Err(_) => true,
            Ok(def) => validate(&def, &source_columns(), &HashMap::new()).is_err(),
        };
        assert!(rejected, "{absent} must not be spellable (yet)");
    }
}

/// `interval_in` reads `IntervalStyle`, which is the input-side reason
/// `INTERVAL` has no typed-literal row: the same text parses to different
/// *values* on two servers, and a definition installed against one must
/// stay correct when read on another.
///
/// Demonstrated rather than asserted, because it is the load-bearing half
/// of that decision.
#[tokio::test]
async fn interval_in_reads_intervalstyle_which_is_why_there_is_no_interval_literal() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let mut parsed = Vec::new();
    for style in ["postgres", "sql_standard"] {
        client
            .batch_execute(&format!("set intervalstyle to '{style}'"))
            .await
            .expect("set intervalstyle");
        // Read the parsed *value* as a number of seconds, so the session's
        // *output* style cannot confound what is an input-side difference.
        let epoch: f64 = client
            .query_one(
                "select extract(epoch from '-1 2:03:04'::interval)::float8",
                &[],
            )
            .await
            .expect("style-sensitive parse")
            .get(0);
        parsed.push(epoch);
    }
    assert_ne!(
        parsed[0], parsed[1],
        "'-1 2:03:04' parses to two different intervals under two IntervalStyles: {parsed:?}"
    );
}

// ---------------------------------------------------------------------
// 7. The live delta path
// ---------------------------------------------------------------------

/// `SUM(interval)` and `MAX(date)` maintained end-to-end through the ring,
/// checked against an independently-authored `SELECT ... GROUP BY` over the
/// live source (ADR-0013 — not `defs::render_aggregate_select_sql`, which is
/// the engine's own renderer).
///
/// Both fields are recompute-only, which is the point: `SUM(interval)` goes
/// through `probe_recompute_fields_bulk`'s server-side
/// `(sum(<col>))::text`, so the value Trellis writes is Postgres's own
/// `sum(interval)` over the group in one pass. That is what makes the
/// infinity and overflow cases in
/// `sum_interval_is_exact_on_finite_values_but_still_recompute_only`
/// unreachable here: there is no accumulation order for them to depend on.
///
/// The insert/update/delete/grain-migration sequence is the same shape
/// `apply_aggregate.rs`'s own oracle test uses; the update and delete are
/// what force a group's value to fall as well as rise.
#[tokio::test]
async fn sum_interval_and_max_date_are_maintained_end_to_end_through_a_drain() {
    use trellis::defs::{create_aggregate_target_table, create_definition};
    use trellis::staging::{StagedWatermark, apply, seal};

    const DEF_SQL: &str = "TRANSFORM shift_totals FROM shifts GROUP BY crew \
         SELECT crew AS crew, SUM(worked) AS total_worked, MAX(on_day) AS last_day";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table shifts ( \
               id integer primary key, crew integer, worked interval, on_day date); \
             alter table shifts replica identity full",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("crew".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("worked".to_string(), ValueType::Other(PgType::Interval)),
        ("on_day".to_string(), ValueType::Other(PgType::Date)),
    ]);
    let def = parse(DEF_SQL).expect("parse");
    validate(&def, &columns, &HashMap::new()).expect("validate");
    create_definition(&db.pool, DEF_SQL, &columns)
        .await
        .expect("create definition");
    create_aggregate_target_table(&db.pool, &def, "public", &columns)
        .await
        .expect("create aggregate target table");

    // The visible columns must be declared with their real Postgres types,
    // not coerced into `numeric` — `SUM(interval)` is `interval` and
    // `MAX(date)` is `date` (`registry::aggregate_result_type`).
    for (column, expected) in [("total_worked", "interval"), ("last_day", "date")] {
        let declared: String = client
            .query_one(
                "select format_type(atttypid, atttypmod) from pg_attribute \
                 where attrelid = 'shift_totals'::regclass and attname = $1",
                &[&column],
            )
            .await
            .unwrap_or_else(|e| panic!("introspect shift_totals.{column}: {e}"))
            .get(0);
        assert_eq!(declared, expected, "shift_totals.{column}");
    }

    /// The independently-authored recompute: plain SQL, hand-written here,
    /// with no reference to the engine's own renderer.
    async fn expected(client: &Client) -> HashMap<String, (Option<String>, Option<String>)> {
        client
            .query(
                "select crew::text, sum(worked)::text, max(on_day)::text \
                 from shifts group by crew",
                &[],
            )
            .await
            .expect("hand-written recompute")
            .into_iter()
            .map(|row| (row.get::<_, String>(0), (row.get(1), row.get(2))))
            .collect()
    }

    async fn actual(client: &Client) -> HashMap<String, (Option<String>, Option<String>)> {
        client
            .query(
                "select crew::text, total_worked::text, last_day::text from shift_totals",
                &[],
            )
            .await
            .expect("read target")
            .into_iter()
            .map(|row| (row.get::<_, String>(0), (row.get(1), row.get(2))))
            .collect()
    }

    async fn stage(
        client: &Client,
        segment: &str,
        key: &str,
        op: &str,
        old_image: Option<&str>,
        new_image: Option<&str>,
    ) {
        let src_table = format!("{DEFAULT_SCHEMA}.shifts");
        client
            .execute(
                &format!(
                    "insert into {segment} \
                     (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                     values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0)"
                ),
                &[
                    &src_table,
                    &key,
                    &op,
                    &testkit::wal_insert_lsn(client).await,
                    &old_image,
                    &new_image,
                ],
            )
            .await
            .unwrap_or_else(|e| panic!("stage {key}: {e}"));
    }

    async fn drain_sealed(client: &mut Client, pool: &trellis::Pool) {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "temporal_worker",
            1,
            "trellis_defs_temporal_test",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something");
    }

    // Step 1 — seed two groups. Crew 1's members are deliberately `=` but
    // spelled differently (`1 day` / `24 hours`), which is exactly the
    // multiset that makes `MAX(interval)` ill-defined and leaves `SUM`
    // perfectly well-defined.
    client
        .batch_execute(
            "insert into shifts (id, crew, worked, on_day) values \
               (1, 1, '1 day',    '2024-01-01'), \
               (2, 1, '24 hours', '2024-01-05'), \
               (3, 2, '2 hours',  '2024-02-01'), \
               (4, 2, '1 mon',    '2023-12-31')",
        )
        .await
        .expect("seed source rows");
    // The staged images carry `interval_out`'s **canonical** spelling,
    // because that is what a real walsender emits — `'24 hours'` reaches
    // the engine as `24:00:00`. Staging the input spelling instead would
    // be testing a shape CDC cannot produce.
    for (key, crew, worked, day) in [
        ("1", "1", "1 day", "2024-01-01"),
        ("2", "1", "24:00:00", "2024-01-05"),
        ("3", "2", "02:00:00", "2024-02-01"),
        ("4", "2", "1 mon", "2023-12-31"),
    ] {
        stage(
            &client,
            "seg_0",
            key,
            "insert",
            None,
            Some(&format!(
                r#"{{"crew":"{crew}","worked":"{worked}","on_day":"{day}"}}"#
            )),
        )
        .await;
    }
    drain_sealed(&mut client, &db.pool).await;
    assert_eq!(
        actual(&client).await,
        expected(&client).await,
        "the seed batch must already match a hand-written recompute"
    );

    // Step 2 — an insert, an in-place update, a grain migration (crew 2 ->
    // 3) and a delete, all in one batch. The update and delete are what
    // drive `SUM`'s *subtraction* leg, which is the half a recompute-only
    // aggregate never exercises.
    client
        .batch_execute(
            "insert into shifts (id, crew, worked, on_day) values \
               (5, 1, '-1 day 1 hour', '2024-03-01'); \
             update shifts set worked = '1 year 2 mons 3 days 04:05:06' where id = 2; \
             update shifts set crew = 3 where id = 3; \
             delete from shifts where id = 4",
        )
        .await
        .expect("apply step 2's live end state");
    stage(
        &client,
        "seg_1",
        "5",
        "insert",
        None,
        Some(r#"{"crew":"1","worked":"-1 days +01:00:00","on_day":"2024-03-01"}"#),
    )
    .await;
    stage(
        &client,
        "seg_1",
        "2",
        "update",
        Some(r#"{"crew":"1","worked":"24:00:00","on_day":"2024-01-05"}"#),
        Some(r#"{"crew":"1","worked":"1 year 2 mons 3 days 04:05:06","on_day":"2024-01-05"}"#),
    )
    .await;
    stage(
        &client,
        "seg_1",
        "3",
        "update",
        Some(r#"{"crew":"2","worked":"02:00:00","on_day":"2024-02-01"}"#),
        Some(r#"{"crew":"3","worked":"02:00:00","on_day":"2024-02-01"}"#),
    )
    .await;
    stage(
        &client,
        "seg_1",
        "4",
        "delete",
        Some(r#"{"crew":"2","worked":"1 mon","on_day":"2023-12-31"}"#),
        None,
    )
    .await;
    drain_sealed(&mut client, &db.pool).await;

    let got = actual(&client).await;
    assert_eq!(
        got,
        expected(&client).await,
        "insert/update/grain-migration/delete must still match a hand-written recompute"
    );

    // And the sum is a real, unjustified interval rather than something
    // that round-tripped through `numeric`: crew 1 holds
    // `1 day + (1 year 2 mons 3 days 04:05:06) + (-1 day +01:00:00)`.
    assert_eq!(
        got["1"].0.as_deref(),
        Some("1 year 2 mons 3 days 05:05:06"),
        "crew 1's interval sum"
    );
}

/// Issue #113's review finding, reproduced end-to-end and pinned as a
/// regression: a `timestamp` `GROUP BY` group whose two source rows reach
/// the engine through *different* row-decode paths must still fold into
/// **one** target row, not two.
///
/// Before issue #248, `staging::apply::read_live_rows_batch` decoded a
/// live-refetched row via `to_jsonb(t.*)`, which spells a `timestamp`
/// `2024-06-15T12:34:56` — a `T` where `::text` (and a real CDC image) renders
/// a space. `staging::apply_aggregate::derive_group_key` reads a change's
/// `GROUP BY` column straight off the decoded `Row`, so a group touched once
/// through an ordinary image-bearing change (space-spelled) and once through
/// a live refetch (`T`-spelled, pre-#248) staged as *two* distinct group-key
/// strings for what is, in Postgres, one single group — the `total = 20`
/// instead of `total = 15` finding from #113's review.
///
/// This reproduces exactly that shape:
///
/// * Row 1 is staged as an ordinary image-bearing `insert` (a real CDC
///   image's own shape — its `grp` text is already canonical, space-spelled).
/// * Row 2 is staged as a bare, image-less `StagedChange::Recompute`
///   (`op = 'recompute'`, both images `NULL` — the shape backfill/reverse-
///   propagation/definition-re-derive all use, per `append.rs`'s own doc
///   comment). `compute()` has no image to decode for it, so it lands in
///   `live_refetch_indices` and is decoded via
///   `staging::apply::read_live_rows_batch` — the exact call site issue #248
///   fixed.
///
/// Both rows share the identical `grp` instant. If the two decode paths ever
/// disagree on its text again, this test fails by finding two rows in
/// `totals` (or a wrong sum) instead of one row summing both contributions —
/// and the *only* way to make it pass by accident (rather than by the fix
/// being correct) would be for Postgres itself to stop distinguishing the
/// two `to_jsonb` spellings, which is not something this crate controls.
#[tokio::test]
async fn a_timestamp_group_key_seeded_by_backfill_and_by_live_read_is_one_group_not_two() {
    use trellis::defs::{create_aggregate_target_table, create_definition};
    use trellis::staging::{StagedWatermark, apply, seal};

    const DEF_SQL: &str =
        "TRANSFORM totals FROM events GROUP BY grp SELECT grp AS grp, SUM(amount) AS total";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table events ( \
               id integer primary key, grp timestamp, amount numeric); \
             alter table events replica identity full",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Other(PgType::Timestamp)),
        ("amount".to_string(), ValueType::Numeric),
    ]);
    let def = parse(DEF_SQL).expect("parse");
    validate(&def, &columns, &HashMap::new()).expect("validate");
    create_definition(&db.pool, DEF_SQL, &columns)
        .await
        .expect("create definition");
    create_aggregate_target_table(&db.pool, &def, "public", &columns)
        .await
        .expect("create aggregate target table");

    // Both rows name the identical Postgres group: the same `grp` instant,
    // down to the microsecond.
    client
        .batch_execute(
            "insert into events (id, grp, amount) values \
               (1, '2024-06-15 12:34:56', 10), \
               (2, '2024-06-15 12:34:56', 5)",
        )
        .await
        .expect("seed source rows");

    async fn stage_image(client: &Client, segment: &str, key: &str, new_image: &str) {
        let src_table = format!("{DEFAULT_SCHEMA}.events");
        client
            .execute(
                &format!(
                    "insert into {segment} \
                     (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                     values ($1, $2, 'insert', $3, null, $4::text::jsonb, 0)"
                ),
                &[
                    &src_table,
                    &key,
                    &testkit::wal_insert_lsn(client).await,
                    &new_image,
                ],
            )
            .await
            .unwrap_or_else(|e| panic!("stage image-bearing {key}: {e}"));
    }

    async fn stage_bare_recompute(client: &Client, segment: &str, key: &str) {
        let src_table = format!("{DEFAULT_SCHEMA}.events");
        client
            .execute(
                &format!(
                    "insert into {segment} \
                     (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                     values ($1, $2, 'recompute', null, null, null, 0)"
                ),
                &[&src_table, &key],
            )
            .await
            .unwrap_or_else(|e| panic!("stage bare recompute {key}: {e}"));
    }

    async fn drain_sealed(client: &mut Client, pool: &trellis::Pool) {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "temporal_worker",
            1,
            "trellis_defs_temporal_issue_113_regression",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something");
    }

    // Row 1: an ordinary image-bearing insert, exactly the shape real CDC
    // produces — canonical, space-spelled `timestamp` text.
    stage_image(
        &client,
        "seg_0",
        "1",
        r#"{"grp":"2024-06-15 12:34:56","amount":"10"}"#,
    )
    .await;
    // Row 2: a bare recompute trigger — no image at all — forcing
    // `read_live_rows_batch`'s live refetch to decode `grp` straight off
    // Postgres.
    stage_bare_recompute(&client, "seg_0", "2").await;

    drain_sealed(&mut client, &db.pool).await;

    let rows = client
        .query("select grp::text, total::text from totals", &[])
        .await
        .expect("read totals");
    assert_eq!(
        rows.len(),
        1,
        "one Postgres GROUP BY group must land as one target row, not two \
         (issue #113's review finding); got {rows:?}",
        rows = rows
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect::<Vec<_>>()
    );
    let (got_grp, got_total): (String, String) = (rows[0].get(0), rows[0].get(1));
    assert_eq!(got_total, "15", "the group's total must be 10 + 5");

    // Cross-check against an independently-authored recompute (ADR-0013):
    // never against the engine's own renderer.
    let expected: Vec<(String, String)> = client
        .query(
            "select grp::text, sum(amount)::text from events group by grp",
            &[],
        )
        .await
        .expect("hand-written recompute")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        expected.len(),
        1,
        "the source itself has exactly one GROUP BY group"
    );
    assert_eq!(expected[0], (got_grp, got_total));
}
