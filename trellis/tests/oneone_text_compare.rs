//! #1303 (ADR-0020): a 1-1 target holds the source's exact rendering, so the
//! apply's upsert skips an update only when the two rows' texts are equal.
//! Comparing by the column type's `=` skipped a write whose value is equal and
//! whose text is not (`numeric` `1.25` to `1.2500`, an `interval` respelled,
//! a float `-0` to `0`, a case change under a case-insensitive collation), and
//! left the copy at the old text for `self_check` to report. Every cell is a
//! 1-1 passthrough, driven by explicit seals and drains (#297).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{PgType, ValueType};
use trellis::integer::IntWidth;
use trellis::staging::self_check::CheckPageExt;
use trellis::{Config, FloatWidth, SelfCheckMode, Trellis, TrellisOptions};

const COLLATION: &str = "create collation public.case_insensitive \
     (provider = icu, locale = 'und-u-ks-level2', deterministic = false);";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

async fn self_check(dsn: &str) -> String {
    let config = Config::with_schema(dsn.to_string(), DEFAULT_SCHEMA).expect("config");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");
    let verdict = match trellis
        .self_check_to_end("c", SelfCheckMode::Strict, Duration::from_secs(3))
        .await
    {
        Ok(report) => format!("{:?}", report.outcome),
        Err(err) => format!("error: {err}"),
    };
    trellis.shutdown().await.expect("shutdown");
    verdict
}

/// The copy's rows (text) that differ from the source's.
async fn diverged(d: &Driver) -> Vec<String> {
    let t = "select id, v::text collate \"C\" from public.t";
    let c = "select id, v::text collate \"C\" from public.c";
    d.rows(&format!(
        "(({c}) except ({t})) union all (({t}) except ({c}))"
    ))
    .await
}

/// A source `t(id, v <ty>)`, seeded with `seed` and drained, then `v` takes
/// `next` (equal to `seed` as a value, different as text) and is drained.
async fn cell(prelude: &str, ty: &str, value_type: ValueType, seed: &str, next: &str) {
    let ddl = format!("{prelude} create table public.t (id integer primary key, v {ty})");
    let mut d = Driver::start(
        &ddl,
        &[("id", int4()), ("v", value_type)],
        &["TRANSFORM c FROM public.t SELECT v AS v"],
        &["public.t"],
    )
    .await;
    if ty.contains("collate") {
        // A passthrough copy takes the database's default collation, never a
        // nondeterministic one, so give the copy the source's by hand: the
        // comparison must be by text wherever a column's `=` is coarser than
        // its text, whatever put the collation there.
        d.ctl
            .batch_execute(
                "alter table public.c alter column v type text collate public.case_insensitive",
            )
            .await
            .expect("collate the copy");
    }
    write(&d, &format!("insert into public.t values (1, {seed})")).await;
    d.settle().await;
    assert_eq!(diverged(&d).await, Vec::<String>::new(), "after the seed");
    write(&d, &format!("update public.t set v = {next} where id = 1")).await;
    d.settle().await;
    assert_eq!(
        diverged(&d).await,
        Vec::<String>::new(),
        "the copy kept the old text after {seed} -> {next}"
    );
    assert_eq!(self_check(d.db.dsn()).await, "Converged");
}

#[tokio::test]
async fn numeric_scale() {
    cell("", "numeric", ValueType::Numeric, "1.25", "1.2500").await;
}

#[tokio::test]
async fn float_negative_zero() {
    cell(
        "",
        "double precision",
        ValueType::Float(FloatWidth::Float8),
        "'-0'",
        "0",
    )
    .await;
    cell(
        "",
        "real",
        ValueType::Float(FloatWidth::Float4),
        "0",
        "'-0'",
    )
    .await;
}

#[tokio::test]
async fn interval_respelled() {
    cell(
        "",
        "interval",
        ValueType::Other(PgType::Interval),
        "'1 day'",
        "'24 hours'",
    )
    .await;
}

#[tokio::test]
async fn case_change_under_a_nondeterministic_collation() {
    cell(
        COLLATION,
        "text collate public.case_insensitive",
        ValueType::Text,
        "'abc'",
        "'ABC'",
    )
    .await;
}

/// `json` has no `=` at all: the old comparison could not even be planned.
#[tokio::test]
async fn json_respelled() {
    cell(
        "",
        "json",
        ValueType::Other(PgType::Json),
        r#"'{"a":1}'"#,
        r#"'{"a": 1}'"#,
    )
    .await;
}

/// A write whose text equals the copy's changes nothing: the row version
/// stays, so a no-op update does not churn the target (WAL, bloat, readers).
#[tokio::test]
async fn an_update_with_equal_text_writes_nothing() {
    let mut d = Driver::start(
        "create table public.t (id integer primary key, v numeric)",
        &[("id", int4()), ("v", ValueType::Numeric)],
        &["TRANSFORM c FROM public.t SELECT v AS v"],
        &["public.t"],
    )
    .await;
    write(&d, "insert into public.t values (1, 1.25)").await;
    d.settle().await;
    let version = d.rows("select xmin::text from public.c").await;
    assert_eq!(version.len(), 1);
    // Out and back within one batch, then the same value again.
    write(&d, "update public.t set v = 9.5 where id = 1").await;
    write(&d, "update public.t set v = 1.25 where id = 1").await;
    d.settle().await;
    write(&d, "update public.t set v = 1.25 where id = 1").await;
    d.settle().await;
    assert_eq!(diverged(&d).await, Vec::<String>::new());
    assert_eq!(
        d.rows("select xmin::text from public.c").await,
        version,
        "a write of equal text rewrote the row"
    );
}

/// Every other type a passthrough admits compares by text without error (a
/// record's text needs only an output function: `xml` and `json` have no
/// `=`), and a respelled or changed value reaches the copy.
#[tokio::test]
async fn the_other_passthrough_types_compare_by_text() {
    let mut d = Driver::start(
        "set lc_monetary = 'C'; \
         create table public.t (id integer primary key, a bytea, b money, c xml, \
             d tsvector, e inet, f bit(4), g jsonb)",
        &[
            ("id", int4()),
            ("a", ValueType::Other(PgType::Bytea)),
            ("b", ValueType::Other(PgType::Money)),
            ("c", ValueType::Other(PgType::Xml)),
            ("d", ValueType::Other(PgType::TsVector)),
            ("e", ValueType::Other(PgType::Inet)),
            ("f", ValueType::Other(PgType::Bit)),
            ("g", ValueType::Other(PgType::Jsonb)),
        ],
        &["TRANSFORM c FROM public.t SELECT a AS a, b AS b, c AS c, d AS d, e AS e, f AS f, g AS g"],
        &["public.t"],
    )
    .await;
    write(
        &d,
        "insert into public.t values (1, '\\x01', '1.00', '<a/>', 'a b', '10.0.0.1', B'0001', \
         '{\"a\": 1}')",
    )
    .await;
    d.settle().await;
    write(
        &d,
        "update public.t set a = '\\x02', b = '2.00', c = '<b/>', d = 'b c', e = '10.0.0.2', \
         f = B'0010', g = '{\"a\": 1.0}' where id = 1",
    )
    .await;
    d.settle().await;
    let all = "select id, a::text, b::text, c::text, d::text, e::text, f::text, g::text \
               from public";
    assert_eq!(
        d.rows(&format!(
            "(({all}.c) except ({all}.t)) union all (({all}.t) except ({all}.c))"
        ))
        .await,
        Vec::<String>::new()
    );
    assert_eq!(self_check(d.db.dsn()).await, "Converged");
}
