//! A rebuild's sweep over a 1-1 target keyed by a text column of an explicit
//! collation (#1520).
//!
//! The ledger's key text (`__from_key`) is `text collate "C"`; a target key
//! keeps its source key's collation, so the key text rendered from a target
//! row carries that collation too. The sweep's orphan walk compares the two,
//! and two different implicit collations left Postgres to choose none: the
//! sweep failed on every attempt and the definition stayed `backfilling`. A column of the database's default
//! collation, or of `"C"`, was unaffected, as was any key whose text kept no
//! collation of its own.
//!
//! A composite key whose parts carry two collations other than the default
//! renders its text under `"C"` (#1522): joined, the parts' texts had no
//! collation Postgres could choose, and the definition's first build failed
//! on every chunk.
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
/// until its backoff passes, so a sweep that fails ends the loop too.
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

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

// ------------------------------------------------------------------ plain 1-1

/// A plain 1-1 target whose key columns are `key` (declared as the column
/// list of `create table public.a (...)`), with `names` the key columns,
/// builds, applies changes, and is checked with no divergence; a rebuild of it
/// finishes and removes the target row no source row backs.
async fn plain_rebuild_removes_the_orphan(
    case: &str,
    key_columns: &str,
    names: &[&str],
    source_rows: &str,
    orphan: &str,
) {
    let mut columns: Vec<(&str, ValueType)> = names.iter().map(|n| (*n, ValueType::Text)).collect();
    columns.push(("v", int4()));
    let key_list = names.join(", ");
    // Started unbuilt, so the test runs the first build by hand and reads
    // where it ended.
    let mut d = Driver::start_unbuilt(
        &[],
        &format!(
            "create table public.a ({key_columns}, v integer, primary key ({key_list})); \
             insert into public.a values {source_rows}"
        ),
        &columns,
        &["TRANSFORM t FROM public.a SELECT a.v AS total"],
        &["public.a"],
    )
    .await;
    work_to_idle(&d).await;
    let (status, error) = status_and_error(&d, "public.t").await;
    assert_eq!(
        (status.as_str(), error),
        ("live", None),
        "{case}: the first build finishes"
    );
    d.user()
        .await
        .batch_execute("update public.a set v = v + 100; delete from public.a where v = 102")
        .await
        .expect("changes to apply");
    d.settle().await;
    let expected = d
        .rows(&format!(
            "select {key_list}, v from public.a order by {key_list}"
        ))
        .await;
    assert_eq!(
        d.rows(&format!(
            "select {key_list}, total from public.t order by {key_list}"
        ))
        .await,
        expected,
        "{case}: the changes apply"
    );
    assert_converged(&d, "public.t", case).await;
    d.ctl
        .batch_execute(&format!(
            "insert into public.t ({key_list}, total) values {orphan}"
        ))
        .await
        .expect("an orphan the source does not back");
    trellis(&d)
        .await
        .request_backfill("public.a")
        .await
        .expect("request a rebuild");
    work_to_idle(&d).await;
    let (status, error) = status_and_error(&d, "public.t").await;
    assert_eq!(
        (status.as_str(), error),
        ("live", None),
        "{case}: the rebuild finishes"
    );
    let actual = d
        .rows(&format!(
            "select {key_list}, total from public.t order by {key_list}"
        ))
        .await;
    assert_eq!(actual, expected, "{case}: the orphan is gone");
}

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

#[tokio::test]
async fn a_rebuild_of_a_key_of_a_non_default_collation_removes_its_orphan() {
    let rows = "('a', 1), ('B', 2), ('c', 3), ('D', 4)";
    for (case, collation) in [
        ("default", ""),
        ("C", "collate \"C\""),
        ("und-x-icu", "collate \"und-x-icu\""),
        ("POSIX", "collate \"POSIX\""),
    ] {
        plain_rebuild_removes_the_orphan(
            case,
            &format!("id text {collation}"),
            &["id"],
            rows,
            "('zz', 99)",
        )
        .await;
    }
}

/// A composite key: the key text is the parts' texts joined, so it takes their
/// collation when they share one, or the one non-default collation beside the
/// default.
#[tokio::test]
async fn a_rebuild_of_a_composite_key_of_a_non_default_collation_removes_its_orphan() {
    for (case, first, second) in [
        ("icu and default", "und-x-icu", "default"),
        ("icu twice", "und-x-icu", "und-x-icu"),
        ("POSIX twice", "POSIX", "POSIX"),
    ] {
        composite_rebuild_removes_the_orphan(case, first, second).await;
    }
}

/// A composite key whose parts carry two different collations other than the
/// default: its key text renders under `"C"` (#1522), so the definition's
/// first build finishes, instead of failing on every chunk.
#[tokio::test]
async fn a_composite_key_of_two_non_default_collations_builds_and_rebuilds() {
    for (case, first, second) in [
        ("icu and POSIX", "und-x-icu", "POSIX"),
        ("C and icu", "C", "und-x-icu"),
    ] {
        composite_rebuild_removes_the_orphan(case, first, second).await;
    }
}

async fn composite_rebuild_removes_the_orphan(case: &str, first: &str, second: &str) {
    plain_rebuild_removes_the_orphan(
        case,
        &format!("id1 text collate \"{first}\", id2 text collate \"{second}\""),
        &["id1", "id2"],
        "('a', 'x', 1), ('B', 'y', 2), ('c', 'X', 3), ('D', 'Y', 4)",
        "('zz', 'zz', 99)",
    )
    .await;
}
