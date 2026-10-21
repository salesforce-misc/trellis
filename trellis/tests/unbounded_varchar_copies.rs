//! A copy of a `varchar(n)` column is an unbounded `varchar` (#1207).
//!
//! Postgres truncates a value whose characters past `n` are all spaces,
//! silently, when it assigns it to a `varchar(n)` column. So after the source
//! widened a `varchar(5)` column, a copy still bounded to 5 would store
//! `'abcde   '` as `'abcde'` when its page drained before the capture pass
//! re-typed the copy: a 1-1 key would upsert into the row of `'abcde'`, a
//! passthrough would lose its spaces. Each test widens the source and writes
//! such a value with no pass in between, then drains. The pipeline and the
//! pass are stepped by hand (`support/drain_driver.rs`), and nothing polls for
//! convergence (#297).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;

const DDL: &str = "create table public.codes (code varchar(5) primary key, \
                       label varchar(5), n integer)";

const ONE_TO_ONE: &str = "TRANSFORM by_code FROM public.codes SELECT label AS label, n AS n";

async fn start() -> Driver {
    Driver::start(
        DDL,
        &[
            ("code", ValueType::Text),
            ("label", ValueType::Text),
            ("n", ValueType::Integer(IntWidth::Int4)),
        ],
        &[ONE_TO_ONE],
        &["public.codes"],
    )
    .await
}

async fn pass(d: &mut Driver) {
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(
        &mut d.ctl,
        &pool,
        DEFAULT_SCHEMA,
        "unbounded_varchar_copies_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

/// The target's rows that differ from the source's, either way.
async fn diverged(d: &Driver) -> Vec<String> {
    d.rows(
        "(select code::text, label::text, n from public.by_code \
          except select code::text, label::text, n from public.codes) \
         union all (select code::text, label::text, n from public.codes \
          except select code::text, label::text, n from public.by_code)",
    )
    .await
}

async fn column_type(d: &Driver, column: &str) -> String {
    d.ctl
        .query_one(
            "select pg_catalog.format_type(atttypid, atttypmod) from pg_catalog.pg_attribute \
             where attrelid = 'public.by_code'::regclass and attname = $1",
            &[&column],
        )
        .await
        .unwrap_or_else(|e| panic!("{column}: {e}"))
        .get(0)
}

async fn status(d: &Driver) -> String {
    d.ctl
        .query_one(
            "select status from transform_definitions where target_table = 'public.by_code'",
            &[],
        )
        .await
        .expect("status")
        .get(0)
}

/// The copies are unbounded and keep the source key's collation; the keys
/// and values are the source's.
#[tokio::test]
async fn the_copies_of_a_varchar_n_are_unbounded() {
    let d = start().await;
    assert_eq!(column_type(&d, "code").await, "character varying");
    assert_eq!(column_type(&d, "label").await, "character varying");
}

/// The source widens `varchar(5)` to `varchar(8)`, and `'abcde   '` (a key of
/// its own, with `'abcde'` beside it) and a padded passthrough drain before
/// any pass: the padded key gets its own row, `'abcde'` keeps its values, the
/// passthrough keeps its spaces, and the target equals the source before and
/// after the next pass.
#[tokio::test]
async fn a_padded_value_after_a_widening_drains_before_the_pass_without_truncation() {
    let mut d = start().await;
    d.user()
        .await
        .batch_execute("insert into public.codes values ('abcde', 'vvvvv', 1)")
        .await
        .expect("seed");
    d.settle().await;
    assert_eq!(diverged(&d).await, Vec::<String>::new());

    d.ctl
        .batch_execute(
            "alter table public.codes alter column code type varchar(8), \
                                      alter column label type varchar(8)",
        )
        .await
        .expect("widen");
    d.user()
        .await
        .batch_execute(
            "insert into public.codes values ('abcde   ', 'vvvvv   ', 2); \
             insert into public.codes values ('fghij', 'wwwww  ', 3)",
        )
        .await
        .expect("write padded values");
    d.settle().await;

    assert_eq!(diverged(&d).await, Vec::<String>::new());
    let rows: Vec<String> = d
        .ctl
        .query(
            "select code::text || '|' || label::text || '|' || n from public.by_code order by 1",
            &[],
        )
        .await
        .expect("read the target")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        rows,
        ["abcde   |vvvvv   |2", "abcde|vvvvv|1", "fghij|wwwww  |3"],
        "the padded key has its own row, and 'abcde' is untouched"
    );

    pass(&mut d).await;
    assert_eq!(status(&d).await, "live");
    d.settle().await;
    assert_eq!(diverged(&d).await, Vec::<String>::new());
}

/// A source `varchar(n)` moved to `varchar(m)`, to `text` or to an unbounded
/// `varchar`, and between those, is no drift: the pass re-types
/// nothing, rebuilds nothing and pauses nothing.
#[tokio::test]
async fn a_varchar_text_change_of_the_source_reports_no_drift() {
    let mut d = start().await;
    d.user()
        .await
        .batch_execute("insert into public.codes values ('abcde', 'vvvvv', 1)")
        .await
        .expect("seed");
    d.settle().await;
    let before: i64 = d
        .ctl
        .query_one(
            "select relfilenode::int8 from pg_class where oid = 'public.by_code'::regclass",
            &[],
        )
        .await
        .expect("filenode")
        .get(0);

    for (code, label) in [
        ("varchar(8)", "varchar(20)"),
        ("text", "text"),
        ("varchar", "varchar"),
        ("text", "varchar"),
        ("varchar", "text"),
    ] {
        d.ctl
            .batch_execute(&format!(
                "alter table public.codes alter column code type {code}, \
                                          alter column label type {label}"
            ))
            .await
            .expect("change the source column types");
        pass(&mut d).await;
        pass(&mut d).await;
        assert_eq!(status(&d).await, "live", "{code} / {label}");
        assert_eq!(column_type(&d, "code").await, "character varying");
        assert_eq!(column_type(&d, "label").await, "character varying");
        let failures: i64 = d
            .ctl
            .query_one("select count(*) from capture_failures", &[])
            .await
            .expect("failures")
            .get(0);
        assert_eq!(failures, 0, "{code} / {label}");
        let requests: i64 = d
            .ctl
            .query_one("select count(*) from retype_releases", &[])
            .await
            .expect("requests")
            .get(0);
        assert_eq!(requests, 0, "{code} / {label}: nothing was re-typed");
    }
    let after: i64 = d
        .ctl
        .query_one(
            "select relfilenode::int8 from pg_class where oid = 'public.by_code'::regclass",
            &[],
        )
        .await
        .expect("filenode")
        .get(0);
    assert_eq!(after, before, "the target was not rewritten");
}
