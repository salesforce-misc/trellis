//! End-to-end tests for the temporal types (`date`, `time`,
//! `timetz`, `timestamp`, `timestamptz`, `interval`) against a real,
//! ephemeral Postgres via `testkit::TestCluster` — including the row-body
//! renderer that agrees with `::text` (so `timestamp` holds every key role
//! `date`/`time`/`timetz` do), and the walsender's output GUCs (including
//! `TimeZone`) pinned the same way the pool's are, which lets `timestamptz`
//! join them too.
//!
//! # Why these run against a live server
//!
//! A typed key index is not automatically the prerequisite for a key
//! role: that premise is wrong for `oid` and right for floats, so each
//! family is a question to be *asked of a
//! server*, rather than answered from the block's name. Every
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
//!    server. `timestamptz` is admitted because Trellis pins `TimeZone` on
//!    the walsender as well as the pool, and
//!    `timestamptz_text_moves_with_the_session_timezone` below still shows
//!    the underlying hazard `TimeZone` pinning now defends against.
//! 3. **The row renderer, both directly and end-to-end.**
//!    `the_engine_s_own_row_renderer_agrees_with_text_for_every_temporal_family`
//!    exercises the actual replacement renderer
//!    (`staging::apply::row_as_text_jsonb_sql`) against `::text`, and
//!    `a_timestamp_group_key_seeded_by_backfill_and_by_live_read_is_one_group_not_two`
//!    reproduces the two-renderer hazard end-to-end through a real drain —
//!    one Postgres `GROUP BY` group, one target row, seeded partly through
//!    an image-bearing change and partly through a bare, image-less
//!    recompute trigger (the shape that used to force
//!    `staging::apply::live_rows_query`'s now-fixed live refetch).
//!
//! Per ADR-0013 every comparison is against independently-authored SQL —
//! plain `pg_typeof`, plain `select ... group by ...`, plain `::text` —
//! never against `defs::oracle::recompute`, which would be the engine's own
//! renderer grading the engine's own evaluator.
//!
//! Harness conventions follow `defs_floats.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::pg_type::PgType;
use trellis::defs::{CatalogError, ValidationError, install_definition, parse, validate};
use trellis::integer::IntWidth;
use trellis::temporal;

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

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

/// Defines a `JOIN` whose one `ON` term pairs `left.lcol` with
/// `right.rcol`. `JOIN` key admission reads the same allowlist
/// (`join_key_types::is_text_stable_join_key_type`) a 1-1 primary key does.
async fn define_join_on(
    pool: &trellis::pool::Pool,
    target: &str,
    (left, lcol): (&str, &str),
    (right, rcol): (&str, &str),
) -> Result<(), trellis::defs::CatalogError> {
    let text = format!(
        "TRANSFORM {target} FROM {left} JOIN {right} ON {left}.{lcol} = {right}.{rcol} \
         GROUP BY {left}.id WITHOUT SOURCE INDEX SELECT COUNT(*) AS n"
    );
    install_definition(pool, &text, &HashMap::new(), "public")
        .await
        .map(|_| ())
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
        // that is the whole shape of the hazard for `timestamp`/
        // `timestamptz`: a single reader groups them perfectly (this probe,
        // one session throughout, measures exactly that), but a *second*
        // renderer could disagree. For `timestamp` that second renderer
        // would be raw `to_jsonb` (see
        // `to_jsonb_and_text_agree_for_every_admitted_key_type`); for
        // `timestamptz` it would additionally be the walsender, whose
        // `TimeZone` Trellis pins
        // (`timestamptz_text_moves_with_the_session_timezone` below still
        // demonstrates the underlying hazard `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`
        // now defends against on the pool and in the capture function). Both
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
/// This is the float `-0`/`0` problem, one family over, and it is why no GUC
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
/// Pinning `TimeZone = 'UTC'` alongside `DateStyle` makes this go away: see
/// `pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`. `TimeZone` is pinned (to `'UTC'`)
/// on every connection Trellis opens, the walsender's included, and
/// `timestamptz` is a text-stable key type. What this test still pins is
/// the underlying, session-scoped fact that makes the pin necessary
/// — an *unpinned* connection's rendering genuinely does
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

// ---------------------------------------------------------------------
// 3. `interval` arithmetic and rendering fidelity
// ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// 4. Aggregates
// ---------------------------------------------------------------------

/// **The guard for the two-renderer hazard.** Every type Trellis admits
/// as a key must render *identically* under both of the engine's two
/// renderers — the per-column `<col>::text` most paths use, and the
/// whole-row `to_jsonb(t.*)`/`jsonb_each_text` the live-row reads use
/// (`staging::apply`'s `live_rows_query`; `staging::quarantine`'s
/// sweep).
///
/// A type that disagrees is one value with two spellings, and a key seeded
/// once through a backfill and once through CDC-then-live-read lands as two
/// target rows for one Postgres group. `timestamp`/`timestamptz` disagree
/// under raw `to_jsonb(t.*)` — it writes an ISO-8601 `T` — which is why
/// **that specific renderer** is never used for a row read any more
/// (`staging::apply::row_as_text_jsonb_sql` replaced it); both
/// are admitted key types today despite this raw disagreement persisting,
/// which this test also pins so a future regression back to raw
/// `to_jsonb(t.*)` fails loudly here instead of shipping silently.
///
/// This sweeps the *whole* allowlist rather than the temporal families, so
/// it also guards every admitted type, and will
/// fail for any family added without checking.
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
        // Controls: both admitted key types (`timestamp`,
        // `timestamptz`) despite raw `to_jsonb(t.*)` disagreeing
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
    // — that is a fact about bare Postgres, and
    // it will stay true forever. What matters is that **nothing inside
    // Trellis calls raw `to_jsonb(t.*)` for a row read any more** (see
    // `the_engine_s_own_row_renderer_agrees_with_text_for_every_temporal_family`,
    // below, which cross-checks the actual renderer
    // `staging::apply::row_as_text_jsonb_sql` now uses), so both are
    // admitted here despite this raw-`to_jsonb` disagreement persisting —
    // `timestamptz` additionally needs the pool and the walsender pinned to
    // the same `TimeZone` (independent of `to_jsonb` vs. `::text`).
    // `interval` is admitted here too (raw `to_jsonb` agrees
    // with `::text` for it) but still refused every key role, because it
    // fails the *other* half of `is_text_stable` — equal values render
    // differently (`'1 day'` vs `'24 hours'`), which no renderer
    // reconciliation can fix.
    assert!(temporal::is_render_consistent(PgType::Timestamp));
    assert!(temporal::is_text_stable(PgType::Timestamp));

    assert!(temporal::is_render_consistent(PgType::TimestampTz));
    assert!(temporal::is_text_stable(PgType::TimestampTz));

    assert!(temporal::is_render_consistent(PgType::Interval));
    assert!(!temporal::is_text_stable(PgType::Interval));
}

/// The engine's row renderer, exercised directly against a live server: the
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
/// `to_jsonb(t.*)`. It deliberately covers `timestamptz` too: the pool vs.
/// the walsender is a *different* renderer pair, handled by pinning
/// `TimeZone`, and this test shows the row renderer has no residual
/// `to_jsonb`-vs-`::text` disagreement of its own.
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

    // The exact call shape `live_rows_query`/`fetch_to_side_rows`/etc.
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
            "{pg_type}: the engine's own row renderer must agree with ::text"
        );
    }
}

// ---------------------------------------------------------------------
// 5. Key roles
// ---------------------------------------------------------------------

/// `date`/`timestamp`/`time`/`timetz`/`timestamptz` are **accepted** as
/// `JOIN` `ON` keys (and therefore as 1-1 primary keys — one
/// allowlist, `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`, gates both), while
/// `interval` alone is refused.
///
/// `date`/`time`/`timetz` need nothing special, `timestamp` is admitted
/// because its `to_jsonb`-vs-`::text` divergence is avoided, and
/// `timestamptz` because the walsender is pinned to the same
/// `TimeZone` as the pool, and the split is the point: the matrix had all
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
               tmtz timetz, tstz timestamptz, iv interval)",
        )
        .await
        .expect("create join tables");

    for (name, from_col, to_table) in [
        ("j_date", "d", "p_date"),
        ("j_tm", "tm", "p_tm"),
        ("j_tmtz", "tmtz", "p_tmtz"),
        // `timestamp` is a bijection under `::text`, but raw `to_jsonb`
        // spells it with an ISO-8601 `T`; the engine's row renderer avoids
        // that (see `to_jsonb_and_text_agree_for_every_admitted_key_type`
        // and `the_engine_s_own_row_renderer_agrees_with_text_for_every_temporal_family`
        // below), so `timestamp` joins the accepted group.
        ("j_ts", "ts", "p_ts"),
        // `timestamptz` has a *second*, independent hazard on top of the
        // `to_jsonb` one above: its rendering moves with `TimeZone`. Trellis
        // pins that on both the pool and the walsender, so `timestamptz`
        // joins the accepted group too.
        ("j_tstz", "tstz", "p_tstz"),
    ] {
        define_join_on(&db.pool, name, ("child", from_col), (to_table, "k"))
            .await
            .unwrap_or_else(|e| panic!("{from_col} must be accepted as a join key: {e}"));
    }

    // `interval` is the one remaining refusal: `'24 hours'` and `'1 day'`
    // are `=` in Postgres but render differently, so no GUC or renderer
    // reconciliation fixes it — see this module's doc comment.
    let err = define_join_on(&db.pool, "j_iv", ("child", "iv"), ("p_iv", "k"))
        .await
        .expect_err("an interval join key must be refused");
    assert!(
        matches!(
            &err,
            CatalogError::Validate(ValidationError::JoinUnsupportedKeyType { pg_type, .. })
                if pg_type == "interval"
        ),
        "the rejection must name the offending type (interval): {err:?}"
    );
}

/// A `timestamp(3)` column must be recognized as `timestamp without time
/// zone` despite `format_type` rendering its modifier *inside* the name.
///
/// This is the concrete reason `join_key_types::base_type_name` replaced
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
    // end-to-end. `timestamp` is an admitted key type, but there's no need
    // to duplicate coverage here —
    // `temporal_join_keys_are_admitted_per_family` already covers plain
    // `timestamp`.)
    client
        .batch_execute(
            "create table p3 (k timetz(2) primary key); \
             create table c3 (id bigint primary key, k timetz(2))",
        )
        .await
        .expect("create modified-precision tables");
    define_join_on(&db.pool, "j3", ("c3", "k"), ("p3", "k"))
        .await
        .expect("a timetz(2) join key must be accepted");
}

/// The `GROUP BY` key gate follows the same five-accepted/one-refused
/// split, and the rejection names the column.
#[test]
fn the_group_by_key_gate_follows_the_same_split() {
    for column in ["d", "tm", "tmtz", "ts", "tstz"] {
        let def = parse(&format!(
            "TRANSFORM t FROM s GROUP BY {column} WITHOUT SOURCE INDEX SELECT {column} AS k, SUM(n) AS total"
        ))
        .expect("parses");
        validate(&def, &source_columns())
            .unwrap_or_else(|e| panic!("{column} must be accepted as a GROUP BY key: {e}"));
    }

    {
        let column = "iv";
        let def = parse(&format!(
            "TRANSFORM t FROM s GROUP BY {column} WITHOUT SOURCE INDEX SELECT {column} AS k, SUM(n) AS total"
        ))
        .expect("parses");
        let err = validate(&def, &source_columns())
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
        // `'12:00:00+05:00:30'` prints as itself.
        "TIMETZ '13:45:00+05:00:30'",
        "TIMETZ '13:45:00+00:00:30'",
        "TIMETZ '13:45:00-00:00:30'",
        "TIMETZ '13:45:00+15:59:59'",
        "CAST('13:45:00+00' AS timetz)",
    ] {
        let def = parse(&format!("TRANSFORM t FROM s SELECT {good} AS x"))
            .unwrap_or_else(|e| panic!("{good} should parse: {e}"));
        validate(&def, &source_columns()).unwrap_or_else(|e| panic!("{good} should validate: {e}"));

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
            Ok(def) => validate(&def, &source_columns()).is_err(),
        };
        assert!(rejected, "{bad} must be rejected");
    }

    // The two temporal families with no literal row at all.
    for absent in ["TIMESTAMPTZ '2024-01-01 00:00:00+00'", "INTERVAL '1 day'"] {
        let parsed = parse(&format!("TRANSFORM t FROM s SELECT {absent} AS x"));
        let rejected = match parsed {
            Err(_) => true,
            Ok(def) => validate(&def, &source_columns()).is_err(),
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

/// The two-renderer hazard, reproduced end-to-end and pinned as a
/// regression: a `timestamp` `GROUP BY` group whose two source rows reach
/// the engine through *different* row-decode paths must still fold into
/// **one** target row, not two.
///
/// Were `staging::apply::live_rows_query` to decode a
/// live-refetched row via `to_jsonb(t.*)`, it would spell a `timestamp`
/// `2024-06-15T12:34:56` — a `T` where `::text` (and a real CDC image) renders
/// a space. A group touched once
/// through an ordinary image-bearing change (space-spelled) and once through
/// a live refetch (`T`-spelled) would then stage as *two* distinct group-key
/// strings for what is, in Postgres, one single group — `total = 20`
/// instead of `total = 15`.
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
///   `staging::apply::live_rows_query` — the exact call site
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
    use trellis::staging::{apply, seal};

    const DEF_SQL: &str = "TRANSFORM totals FROM events GROUP BY grp WITHOUT SOURCE INDEX SELECT grp AS grp, SUM(amount) AS total";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table events ( \
               id integer primary key, grp timestamp, amount numeric(12,0))",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Other(PgType::Timestamp)),
        ("amount".to_string(), ValueType::Numeric),
    ]);
    let def = parse(DEF_SQL).expect("parse");
    validate(&def, &columns).expect("validate");
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
    // `live_rows_query`'s live refetch to decode `grp` straight off
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
        "one Postgres GROUP BY group must land as one target row, not two; got {rows:?}",
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
