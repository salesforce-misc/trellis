//! A 1-1 target's fields are evaluated by Postgres, in the upsert that
//! writes them (ADR-0002, `staging::one_to_one_render`), and a value a field
//! can't be computed from is isolated to its record and its column
//! (ADR-0003, "What the column fuse counts").
//!
//! Every test stages its page into the ring by hand, as `quarantine.rs`
//! does, and drives it with `drain_once`: deterministic, with no
//! convergence polling.

#[path = "support/render_lab.rs"]
mod render_lab;

use std::collections::HashMap;

use render_lab::{connect_raw, define, drain, insert_source, is_data_exception, stage};
use testkit::{TestCluster, TestDatabase};
use tokio_postgres::Client;
use trellis::defs::ast::ValueType;
use trellis::defs::create_definition;
use trellis::float::FloatWidth;
use trellis::integer::IntWidth;
use trellis::staging::apply::ApplyError;
use trellis::staging::quarantine::{
    DEFAULT_COLUMN_DEATH_THRESHOLD, DEFAULT_DEATH_THRESHOLD, FailureClass, classify,
};

/// Clean records in each poison test's page, around its poisoned one.
const PAGE: i32 = 10_000;

/// Drains `seg_seq` until it applies, at most `attempts` times, and returns
/// how many drains failed with a data exception first. Any other failure
/// fails the test.
async fn drain_until_applied(db: &TestDatabase, seg_seq: i64, attempts: usize) -> usize {
    for failed in 0..attempts {
        match drain(db, seg_seq).await {
            Ok(_) => return failed,
            Err(err) if is_data_exception(&err) => {}
            Err(err) => panic!("drain {failed} failed with something else: {err:?}"),
        }
    }
    panic!("the page didn't apply in {attempts} drains");
}

async fn count(client: &Client, sql: &str) -> i64 {
    client
        .query_one(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .get(0)
}

async fn poisoned_keys(client: &Client) -> Vec<String> {
    client
        .query("select key from poison order by key", &[])
        .await
        .expect("read poison")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

async fn column_failure_keys(client: &Client, transform: &str, column: &str) -> Vec<String> {
    client
        .query(
            "select key from column_failures \
             where transform_table = $1 and column_name = $2 order by key",
            &[&transform, &column],
        )
        .await
        .expect("read column_failures")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

async fn column_paused(client: &Client, transform: &str, column: &str) -> bool {
    client
        .query_opt(
            "select 1 from column_status where transform_table = $1 and column_name = $2",
            &[&transform, &column],
        )
        .await
        .expect("read column_status")
        .is_some()
}

/// `int4_col + 1` is `integer`, as Postgres types it and as define
/// declared the target column: it overflows at `integer`'s bound. In a
/// page of 10,000 records, the one at the bound is isolated, charged to its
/// column and, at the death threshold, held; every other record applies,
/// one below the bound included.
#[tokio::test]
async fn an_integer_overflow_quarantines_only_its_record() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    define(
        &db,
        &client,
        "nums",
        "id integer primary key, a integer",
        "TRANSFORM nums_next FROM public.nums SELECT a + 1 AS b",
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("a", ValueType::Integer(IntWidth::Int4)),
        ],
    )
    .await;
    let poisoned = PAGE / 2;
    let rows: Vec<(String, String)> = (1..=PAGE)
        .map(|id| {
            let a = match id {
                _ if id == poisoned => i32::MAX as i64,
                1 => i32::MAX as i64 - 1,
                _ => id as i64,
            };
            (id.to_string(), format!(r#"{{"id":"{id}","a":"{a}"}}"#))
        })
        .collect();
    let seg_seq = stage(&mut client, "nums", &rows).await;

    let failed = drain_until_applied(&db, seg_seq, DEFAULT_DEATH_THRESHOLD as usize + 1).await;
    assert_eq!(
        failed,
        DEFAULT_DEATH_THRESHOLD as usize - 1,
        "each drain charges the record once; the drain that reaches the threshold holds it \
         and applies the rest, so it doesn't fail"
    );
    assert_eq!(poisoned_keys(&client).await, vec![poisoned.to_string()]);
    assert_eq!(
        column_failure_keys(&client, "nums_next", "b").await,
        vec![poisoned.to_string()],
        "the per-field probe names the failing column"
    );
    assert!(!column_paused(&client, "nums_next", "b").await);
    assert_eq!(
        count(&client, "select count(*) from public.nums_next").await,
        PAGE as i64 - 1
    );
    let one_below: i32 = client
        .query_one(
            "select b from public.nums_next where id = 1 and pg_typeof(b) = 'integer'::regtype",
            &[],
        )
        .await
        .expect("read the row one below the bound")
        .get(0);
    assert_eq!(
        one_below,
        i32::MAX,
        "one below the bound is the bound, in an integer column"
    );
    assert_eq!(
        count(
            &client,
            "select count(*) from public.nums_next where id <> 1 and b is distinct from id + 1"
        )
        .await,
        0
    );
}

/// One record of a page raises in the field's arithmetic (`22003` overflow,
/// `22012` division by zero): Postgres computes the field, so the record is
/// isolated, charged to its column (the per-field probe names `v`) and, at the
/// death threshold, held, and every other record applies. `columns` are the
/// source's data columns (DDL and type, in order); `image` is the record
/// `id`'s values, as the row image carries them.
async fn arithmetic_error_quarantines_only_its_record(
    expr: &str,
    columns: &[(&str, &str, ValueType)],
    poisoned: i32,
    image: impl Fn(i32) -> Vec<String>,
) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    let ddl: Vec<String> = columns.iter().map(|(n, t, _)| format!("{n} {t}")).collect();
    let mut types = vec![("id", ValueType::Integer(IntWidth::Int4))];
    types.extend(columns.iter().map(|(n, _, t)| (*n, *t)));
    define(
        &db,
        &client,
        "ops",
        &format!("id integer primary key, {}", ddl.join(", ")),
        &format!("TRANSFORM ops_out FROM public.ops SELECT {expr} AS v"),
        &types,
    )
    .await;
    const ROWS: i32 = 300;
    let rows: Vec<(String, String)> = (1..=ROWS)
        .map(|id| {
            let cells: Vec<String> = columns
                .iter()
                .zip(image(id))
                .map(|((name, _, _), value)| format!(r#""{name}":"{value}""#))
                .collect();
            (
                id.to_string(),
                format!(r#"{{"id":"{id}",{}}}"#, cells.join(",")),
            )
        })
        .collect();
    assert!(rows.iter().any(|(k, _)| *k == poisoned.to_string()));
    let seg_seq = stage(&mut client, "ops", &rows).await;
    let failed = drain_until_applied(&db, seg_seq, DEFAULT_DEATH_THRESHOLD as usize + 1).await;
    assert_eq!(failed, DEFAULT_DEATH_THRESHOLD as usize - 1, "`{expr}`");
    assert_eq!(
        poisoned_keys(&client).await,
        vec![poisoned.to_string()],
        "`{expr}`"
    );
    assert_eq!(
        column_failure_keys(&client, "ops_out", "v").await,
        vec![poisoned.to_string()],
        "`{expr}`: the per-field probe names the failing column"
    );
    assert_eq!(
        count(&client, "select count(*) from public.ops_out").await,
        ROWS as i64 - 1,
        "`{expr}`"
    );
}

const INT4: ValueType = ValueType::Integer(IntWidth::Int4);

#[tokio::test]
async fn an_integer_product_overflow_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record("a * 2", &[("a", "integer", INT4)], 7, |id| {
        vec![if id == 7 { i32::MAX } else { id }.to_string()]
    })
    .await;
}

#[tokio::test]
async fn a_smallint_product_overflow_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record(
        "s * s",
        &[("s", "smallint", ValueType::Integer(IntWidth::Int2))],
        9,
        |id| vec![if id == 9 { 200 } else { id % 100 }.to_string()],
    )
    .await;
}

#[tokio::test]
async fn an_integer_difference_overflow_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record("a - 1", &[("a", "integer", INT4)], 11, |id| {
        vec![if id == 11 { i32::MIN } else { id }.to_string()]
    })
    .await;
}

const DATE: ValueType = ValueType::Other(trellis::defs::PgType::Date);
const TIMESTAMP: ValueType = ValueType::Other(trellis::defs::PgType::Timestamp);

/// A date past the latest `date` (`d + 1` of 5874897-12-31): Postgres raises
/// `22008` for the record that holds it, and every other record applies.
#[tokio::test]
async fn a_date_past_the_latest_date_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record("d + 1", &[("d", "date", DATE)], 17, |id| {
        vec![if id == 17 {
            "5874897-12-31".to_string()
        } else {
            "2024-02-29".to_string()
        }]
    })
    .await;
}

#[tokio::test]
async fn a_date_before_the_earliest_date_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record("d - 1", &[("d", "date", DATE)], 19, |id| {
        vec![if id == 19 {
            "4714-11-24 BC".to_string()
        } else {
            "2024-02-29".to_string()
        }]
    })
    .await;
}

/// An integer day count that takes a date out of range.
#[tokio::test]
async fn a_day_count_that_leaves_the_date_range_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record(
        "d + n",
        &[("d", "date", DATE), ("n", "integer", INT4)],
        21,
        |id| {
            vec![
                "2024-02-29".to_string(),
                if id == 21 { i32::MAX } else { id }.to_string(),
            ]
        },
    )
    .await;
}

/// The days between two dates are undefined when one is infinite
/// (`cannot subtract infinite dates`, `22008`); a finite pair and an
/// infinite `date + integer` are values.
#[tokio::test]
async fn subtracting_an_infinite_date_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record(
        "d - e",
        &[("d", "date", DATE), ("e", "date", DATE)],
        23,
        |id| {
            vec![
                if id == 23 { "infinity" } else { "2024-02-29" }.to_string(),
                "2024-01-01".to_string(),
            ]
        },
    )
    .await;
}

/// A date past what a `timestamp` holds (294276-12-31) raises `22008` in the
/// cast; the dates below it, `infinity` included, cast.
#[tokio::test]
async fn casting_a_date_past_the_timestamp_range_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record(
        "CAST(d AS timestamp)",
        &[("d", "date", DATE)],
        25,
        |id| {
            vec![
                match id {
                    25 => "294277-01-01",
                    26 => "infinity",
                    27 => "294276-12-31",
                    _ => "2024-02-29",
                }
                .to_string(),
            ]
        },
    )
    .await;
}

/// Truncating the earliest timestamps to a millennium leaves the range
/// (`timestamp out of range`, `22008`).
#[tokio::test]
async fn truncating_a_timestamp_out_of_range_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record(
        "DATE_TRUNC('millennium', ts)",
        &[("ts", "timestamp", TIMESTAMP)],
        27,
        |id| {
            vec![
                if id == 27 {
                    "4713-01-01 00:00:00 BC"
                } else {
                    "2024-02-29 12:00:00"
                }
                .to_string(),
            ]
        },
    )
    .await;
}

#[tokio::test]
async fn negating_the_smallest_integer_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record("-a", &[("a", "integer", INT4)], 13, |id| {
        vec![if id == 13 { i32::MIN } else { id }.to_string()]
    })
    .await;
}

#[tokio::test]
async fn the_absolute_value_of_the_smallest_integer_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record("ABS(a)", &[("a", "integer", INT4)], 15, |id| {
        vec![if id == 15 { i32::MIN } else { id }.to_string()]
    })
    .await;
}

#[tokio::test]
async fn a_numeric_division_by_zero_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record(
        "n / d",
        &[
            ("n", "numeric", ValueType::Numeric),
            ("d", "numeric", ValueType::Numeric),
        ],
        17,
        |id| {
            vec![
                "1.5".to_string(),
                if id == 17 { "0" } else { "2" }.to_string(),
            ]
        },
    )
    .await;
}

#[tokio::test]
async fn an_integer_remainder_by_zero_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record(
        "a % d",
        &[("a", "integer", INT4), ("d", "integer", INT4)],
        19,
        |id| vec![id.to_string(), if id == 19 { 0 } else { 3 }.to_string()],
    )
    .await;
}

#[tokio::test]
async fn a_float_division_by_zero_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record(
        "x / y",
        &[
            (
                "x",
                "double precision",
                ValueType::Float(FloatWidth::Float8),
            ),
            (
                "y",
                "double precision",
                ValueType::Float(FloatWidth::Float8),
            ),
        ],
        21,
        |id| {
            vec![
                "1.5".to_string(),
                if id == 21 { "0" } else { "4" }.to_string(),
            ]
        },
    )
    .await;
}

/// A `CASE` branch that raises (`22012`, `22003`) fails only the records that
/// take it: a record whose condition is false never evaluates it, so it
/// applies. The poisoned record is charged to the column.
#[tokio::test]
async fn a_case_branch_division_by_zero_quarantines_only_the_records_that_take_it() {
    // Records 24 to 26 have a zero divisor too, but their condition is false.
    arithmetic_error_quarantines_only_its_record(
        "CASE WHEN a > 0 THEN n / d ELSE 0 END",
        &[
            ("a", "integer", INT4),
            ("n", "numeric", ValueType::Numeric),
            ("d", "numeric", ValueType::Numeric),
        ],
        23,
        |id| {
            let (a, d) = match id {
                23 => (1, 0),
                24..=26 => (-1, 0),
                _ => (1, 2),
            };
            vec![a.to_string(), "1.5".to_string(), d.to_string()]
        },
    )
    .await;
}

#[tokio::test]
async fn a_case_branch_overflow_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record(
        "CASE WHEN a > 0 THEN a * 2 WHEN a < 0 THEN a ELSE 0 END",
        &[("a", "integer", INT4)],
        29,
        |id| vec![if id == 29 { i32::MAX } else { id % 100 }.to_string()],
    )
    .await;
}

#[tokio::test]
async fn an_else_branch_error_quarantines_only_its_record() {
    arithmetic_error_quarantines_only_its_record(
        "CASE WHEN a > 0 THEN 1 ELSE a - 1 END",
        &[("a", "integer", INT4)],
        31,
        |id| vec![if id == 31 { i32::MIN } else { id }.to_string()],
    )
    .await;
}

/// `x / NULLIF(y, 0)` is the guard: a zero divisor gives `NULL`, no record
/// fails, and none is quarantined.
#[tokio::test]
async fn nullif_guards_a_division_so_no_record_is_quarantined() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    define(
        &db,
        &client,
        "guarded",
        "id integer primary key, n numeric, d numeric",
        "TRANSFORM guarded_out FROM public.guarded SELECT n / NULLIF(d, 0) AS v",
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("n", ValueType::Numeric),
            ("d", ValueType::Numeric),
        ],
    )
    .await;
    let rows: Vec<(String, String)> = (1..=300)
        .map(|id| {
            let d = if id % 10 == 0 { "0" } else { "2" };
            (
                id.to_string(),
                format!(r#"{{"id":"{id}","n":"3","d":"{d}"}}"#),
            )
        })
        .collect();
    let seg_seq = stage(&mut client, "guarded", &rows).await;
    let failed = drain_until_applied(&db, seg_seq, 2).await;
    assert_eq!(failed, 0);
    assert!(poisoned_keys(&client).await.is_empty());
    assert_eq!(
        count(&client, "select count(*) from public.guarded_out").await,
        300
    );
    assert_eq!(
        count(
            &client,
            "select count(*) from public.guarded_out where v is null"
        )
        .await,
        30
    );
}

/// `COALESCE(r, n)` casts `n` to `real` where `r` is `NULL`, which raises
/// `22003` for an `n` outside `real`'s range, per row. Only that row's
/// record is isolated; its batch-mates, `n` in range, apply.
#[tokio::test]
async fn a_value_out_of_a_casts_range_quarantines_only_its_record() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    define(
        &db,
        &client,
        "fallbacks",
        "id integer primary key, r real, n numeric",
        "TRANSFORM fallbacks_out FROM public.fallbacks SELECT COALESCE(r, n) AS v",
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("r", ValueType::Float(FloatWidth::Float4)),
            ("n", ValueType::Numeric),
        ],
    )
    .await;
    let poisoned = 7;
    let rows: Vec<(String, String)> = (1..=PAGE)
        .map(|id| {
            let n = if id == poisoned { "1e39" } else { "2.5" };
            (
                id.to_string(),
                format!(r#"{{"id":"{id}","r":null,"n":"{n}"}}"#),
            )
        })
        .collect();
    let seg_seq = stage(&mut client, "fallbacks", &rows).await;

    let failed = drain_until_applied(&db, seg_seq, DEFAULT_DEATH_THRESHOLD as usize + 1).await;
    assert_eq!(failed, DEFAULT_DEATH_THRESHOLD as usize - 1);
    assert_eq!(poisoned_keys(&client).await, vec![poisoned.to_string()]);
    assert_eq!(
        column_failure_keys(&client, "fallbacks_out", "v").await,
        vec![poisoned.to_string()]
    );
    assert_eq!(
        count(
            &client,
            "select count(*) from public.fallbacks_out where v = 2.5::real"
        )
        .await,
        PAGE as i64 - 1
    );
}

/// A row image without a column a field reads (one staged before a capture
/// widen, which the capture gate otherwise keeps from the reader) fails
/// its record with a data exception: it is isolated, charged to the field
/// and held, and never written as if the column were `NULL`. A column
/// present with a `NULL` value reads `NULL`.
#[tokio::test]
async fn a_row_without_a_column_a_field_reads_is_isolated_not_read_as_null() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    define(
        &db,
        &client,
        "partial",
        "id integer primary key, a integer",
        "TRANSFORM partial_out FROM public.partial SELECT COALESCE(a, 0) + 1 AS b",
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("a", ValueType::Integer(IntWidth::Int4)),
        ],
    )
    .await;
    let (missing, null) = (7, 8);
    let rows: Vec<(String, String)> = (1..=50)
        .map(|id| {
            let image = match id {
                _ if id == missing => format!(r#"{{"id":"{id}"}}"#),
                _ if id == null => format!(r#"{{"id":"{id}","a":null}}"#),
                _ => format!(r#"{{"id":"{id}","a":"{id}"}}"#),
            };
            (id.to_string(), image)
        })
        .collect();
    let seg_seq = stage(&mut client, "partial", &rows).await;

    let failed = drain_until_applied(&db, seg_seq, DEFAULT_DEATH_THRESHOLD as usize + 1).await;
    assert_eq!(failed, DEFAULT_DEATH_THRESHOLD as usize - 1);
    assert_eq!(poisoned_keys(&client).await, vec![missing.to_string()]);
    assert_eq!(
        column_failure_keys(&client, "partial_out", "b").await,
        vec![missing.to_string()]
    );
    assert_eq!(
        count(
            &client,
            &format!("select count(*) from public.partial_out where id = {missing}")
        )
        .await,
        0,
        "the row without the column is not written"
    );
    assert_eq!(
        count(
            &client,
            &format!("select count(*) from public.partial_out where id = {null} and b = 1")
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &client,
            "select count(*) from public.partial_out where b = id + 1"
        )
        .await,
        48
    );
}

/// A pattern Postgres refuses (`2201B`) fails every record. Define refuses
/// such a pattern, so the test plants it in the catalog after define, as a
/// server whose verdict changed would leave it. Each one isolation pins is a distinct
/// failing row of the column, so the column fuse trips at its threshold,
/// the column pauses, and the page applies without it: the healthy field is
/// written for every record and no key is held.
#[tokio::test]
async fn an_invalid_regex_trips_its_column_fuse_and_the_rest_applies() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    define(
        &db,
        &client,
        "notes",
        "id integer primary key, t text",
        "TRANSFORM notes_out FROM public.notes \
         SELECT REGEXP_COUNT(t, 'zzz') AS named, CHAR_LENGTH(t) AS len",
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("t", ValueType::Text),
        ],
    )
    .await;
    client
        .execute(
            "update trellis.transform_definitions \
             set definition_text = replace(definition_text, 'zzz', '(?P<n>a)')",
            &[],
        )
        .await
        .expect("plant the pattern");
    let rows: Vec<(String, String)> = (1..=200)
        .map(|id| (id.to_string(), format!(r#"{{"id":"{id}","t":"abc{id}"}}"#)))
        .collect();
    let seg_seq = stage(&mut client, "notes", &rows).await;

    drain_until_applied(&db, seg_seq, 3).await;
    assert!(column_paused(&client, "notes_out", "named").await);
    assert!(!column_paused(&client, "notes_out", "len").await);
    assert_eq!(
        column_failure_keys(&client, "notes_out", "named")
            .await
            .len(),
        DEFAULT_COLUMN_DEATH_THRESHOLD as usize,
        "isolation pins the threshold's worth of distinct rows, and the pause ends it"
    );
    assert!(poisoned_keys(&client).await.is_empty());
    assert_eq!(
        count(&client, "select count(*) from key_deaths").await,
        0,
        "once the column pauses, no record fails, so none is charged"
    );
    assert_eq!(
        count(
            &client,
            "select count(*) from public.notes_out where len = char_length('abc' || id)"
        )
        .await,
        200
    );
}

/// `REGEXP_COUNT` runs under the column's own collation, as over the source
/// table (#643): `\w` matches `é` under `"C.utf8"` and not under `"C"`.
#[tokio::test]
async fn regexp_count_runs_under_the_columns_collation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    define(
        &db,
        &client,
        "words",
        r#"id integer primary key, tc text collate "C", tu text collate "C.utf8""#,
        r"TRANSFORM words_out FROM public.words SELECT REGEXP_COUNT(tc, '\w') AS wc, REGEXP_COUNT(tu, '\w') AS wu",
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("tc", ValueType::Text),
            ("tu", ValueType::Text),
        ],
    )
    .await;
    let rows = vec![(
        "1".to_string(),
        r#"{"id":"1","tc":"é","tu":"é"}"#.to_string(),
    )];
    insert_source(&client, "words", &rows).await;
    let seg_seq = stage(&mut client, "words", &rows).await;
    drain(&db, seg_seq).await.expect("drain");

    let row = client
        .query_one("select wc, wu from public.words_out where id = 1", &[])
        .await
        .expect("read the target");
    assert_eq!((row.get::<_, i32>(0), row.get::<_, i32>(1)), (0, 1));
    assert_eq!(
        count(
            &client,
            r"select count(*) from public.words s join public.words_out t using (id)
              where t.wc = regexp_count(s.tc, '\w') and t.wu = regexp_count(s.tu, '\w')"
        )
        .await,
        1,
        "the target holds what Postgres computes over the source"
    );
}

/// `COALESCE` over a `real` takes Postgres's result type, so its value is
/// Postgres's (#1418's review): over a `NULL` `r`, `COALESCE(r, 0.1) > 0.1`
/// compares the `real` 0.1 in double precision, and the sums are `double
/// precision` of a `real` fallback.
#[tokio::test]
async fn coalesce_over_a_real_computes_what_postgres_computes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    define(
        &db,
        &client,
        "reals",
        "id integer primary key, r real",
        "TRANSFORM reals_out FROM public.reals SELECT \
         (COALESCE(r, 0.1) > 0.1) AS gt, (COALESCE(r, 0.1) + 0) AS p, \
         (COALESCE(r, 16777217) + 0) AS q",
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("r", ValueType::Float(FloatWidth::Float4)),
        ],
    )
    .await;
    let rows = vec![("1".to_string(), r#"{"id":"1","r":null}"#.to_string())];
    insert_source(&client, "reals", &rows).await;
    let seg_seq = stage(&mut client, "reals", &rows).await;
    drain(&db, seg_seq).await.expect("drain");

    let row = client
        .query_one(
            "select gt, p::text, q::text from public.reals_out where id = 1",
            &[],
        )
        .await
        .expect("read the target");
    assert_eq!(
        (
            row.get::<_, bool>(0),
            row.get::<_, String>(1),
            row.get::<_, String>(2)
        ),
        (
            true,
            "0.10000000149011612".to_string(),
            "16777216".to_string()
        )
    );
}

/// A `COALESCE` fallback literal outside the range of the float it is cast to
/// is refused at define, because Postgres would raise `22003` for it at plan
/// time for every row. An in-range literal and a column fallback define.
#[tokio::test]
async fn a_coalesce_literal_out_of_reals_range_is_refused_at_define() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute("create table public.wide (id integer primary key, r real, n numeric)")
        .await
        .expect("create the source");
    let columns: HashMap<String, ValueType> = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("r".to_string(), ValueType::Float(FloatWidth::Float4)),
        ("n".to_string(), ValueType::Numeric),
    ]);
    let beyond_real = format!("1{}", "0".repeat(39));
    let refused = create_definition(
        &db.pool,
        &format!("TRANSFORM wide_out FROM public.wide SELECT COALESCE(r, {beyond_real}) AS v"),
        &columns,
    )
    .await
    .expect_err("an out-of-range fallback literal is refused");
    let message = refused.to_string();
    assert!(
        message.contains("out of range for real") && message.contains(&beyond_real),
        "{message}"
    );
    // A constant sum Postgres cannot compute fails for every row, so define
    // refuses it too.
    let overflow = create_definition(
        &db.pool,
        "TRANSFORM sum_out FROM public.wide SELECT 2147483647 + 1 AS v",
        &columns,
    )
    .await
    .expect_err("an overflowing constant sum is refused");
    assert!(
        overflow.to_string().contains("out of range for integer"),
        "{overflow}"
    );
    for (i, fallback) in ["3", "n"].into_iter().enumerate() {
        create_definition(
            &db.pool,
            &format!("TRANSFORM ok{i}_out FROM public.wide SELECT COALESCE(r, {fallback}) AS v"),
            &columns,
        )
        .await
        .expect("an in-range literal and a column fallback define");
    }
}

/// A lock timeout (`55P03`) is transient: it is retried, never isolated and
/// never charged to a record or a column, while a data exception (`22003`)
/// from the same kind of statement is isolated. Both errors are real ones,
/// from Postgres.
#[tokio::test]
async fn a_lock_timeout_is_never_bisected_and_a_data_exception_is() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let holder = connect_raw(db.dsn()).await;
    let waiter = connect_raw(db.dsn()).await;
    holder
        .batch_execute("create table public.locked (id integer primary key)")
        .await
        .expect("create the table");
    holder
        .batch_execute("begin; lock table public.locked")
        .await
        .expect("hold the lock");
    waiter
        .batch_execute("set lock_timeout = '10ms'")
        .await
        .expect("set the timeout");
    let timed_out = ApplyError::Db(
        waiter
            .batch_execute("begin; lock table public.locked")
            .await
            .expect_err("the lock wait times out"),
    );
    assert!(!is_data_exception(&timed_out));
    assert!(matches!(classify(&timed_out), FailureClass::Transient));

    waiter
        .batch_execute("rollback")
        .await
        .expect("leave the aborted transaction");
    let overflow = ApplyError::Db(
        waiter
            .query_one("select (2147483647 + $1::integer)", &[&1_i32])
            .await
            .expect_err("the sum overflows"),
    );
    assert!(is_data_exception(&overflow));
    assert!(matches!(classify(&overflow), FailureClass::Isolate));
}
