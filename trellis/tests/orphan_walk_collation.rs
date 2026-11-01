//! A rebuild's sweep over a 1-1 target keyed by a text column of an explicit
//! collation (#1520).
//!
//! The ledger's key text (`__from_key`, and `R`'s and `M`'s for a 1-1 `JOIN`)
//! is `text collate "C"`; a target key keeps its source key's collation, so the
//! key text rendered from a target row carries that collation too. The sweep's
//! orphan walk compares the two, and two different implicit collations left
//! Postgres to choose none: the sweep failed on every attempt and the
//! definition stayed `backfilling`. A column of the database's default
//! collation, or of `"C"`, was unaffected, as was any key whose text kept no
//! collation of its own.
//!
//! A composite key whose parts carry two collations other than the default
//! renders its text under `"C"` (#1522): joined, the parts' texts had no
//! collation Postgres could choose, and the definition's first build failed
//! on every chunk.
//!
//! A source key re-collated after define between deterministic collations
//! leaves the target's key columns at the collations they were created with
//! (#1537). Every composite key's text is `"C"`, so a statement that renders
//! one table's key over the other's rows, or compares the two, finds one
//! collation whatever either table's parts carry.
//!
//! Each test steps the build by hand (`build::work_once`) to idle and asserts
//! where the definition ended: no test waits on convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::Config;
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, Step, WorkerOptions};
use trellis::staging::self_check::{SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page};
use trellis::{Trellis, TrellisOptions};

/// The `JOIN` tests turn the dev knob on, which is process-wide.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    let mut d = Driver::start(
        &format!(
            "create table public.a ({key_columns}, v integer, primary key ({key_list})); \
             insert into public.a values {source_rows}"
        ),
        &columns,
        &["TRANSFORM t FROM public.a SELECT a.v AS total"],
        &["public.a"],
    )
    .await;
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
/// collation when they share one, the one non-default collation beside the
/// default, and `"C"` when two non-default ones meet (#1522).
#[tokio::test]
async fn a_rebuild_of_a_composite_key_of_a_non_default_collation_removes_its_orphan() {
    let rows = "('a', 'x', 1), ('B', 'y', 2), ('c', 'X', 3), ('D', 'Y', 4)";
    for (case, first, second) in [
        ("icu and default", "und-x-icu", "default"),
        ("icu twice", "und-x-icu", "und-x-icu"),
        ("POSIX twice", "POSIX", "POSIX"),
        ("icu and POSIX", "und-x-icu", "POSIX"),
        ("C and icu", "C", "und-x-icu"),
    ] {
        plain_rebuild_removes_the_orphan(
            case,
            &format!("id1 text collate \"{first}\", id2 text collate \"{second}\""),
            &["id1", "id2"],
            rows,
            "('zz', 'zz', 99)",
        )
        .await;
    }
}

// ------------------------------------------------- re-collated after define

/// One re-collation case: the source's key columns as created, its rows,
/// the key columns' names, a target row no source row backs, and the
/// `alter table public.a ...` run once the definition is built.
struct Recollation<'a> {
    case: &'a str,
    key_columns: String,
    names: &'a [&'a str],
    source_rows: &'a str,
    orphan: &'a str,
    recollate: &'a str,
}

/// Every composite case's rows: parts that order otherwise under
/// `und-x-icu` than under `"POSIX"` or `"C"` (`B` sorts after `a` under ICU,
/// before it by bytes).
const COMPOSITE_ROWS: &str = "('a', 'x', 1), ('B', 'y', 2), ('c', 'X', 3), ('D', 'Y', 4), \
                              ('a', 'Y', 5), ('B', 'x', 6)";

impl Recollation<'_> {
    fn composite(case: &'static str, first: &str, second: &str, recollate: &'static str) -> Self {
        Recollation {
            case,
            key_columns: format!("id1 text collate \"{first}\", id2 text collate \"{second}\""),
            names: &["id1", "id2"],
            source_rows: COMPOSITE_ROWS,
            orphan: "('zz', 'zz', 99)",
            recollate,
        }
    }
}

/// The source's, the target's and the reader's rows, each in one order.
struct Tables {
    source: String,
    target: String,
    reader: String,
}

impl Tables {
    /// The target and its reader hold the source's rows, and a check of
    /// each finds no divergence.
    async fn assert_rebuilt(&self, d: &Driver, case: &str, stage: &str) {
        let expected = d.rows(&self.source).await;
        assert_eq!(d.rows(&self.target).await, expected, "{case}: {stage}: t");
        assert_eq!(d.rows(&self.reader).await, expected, "{case}: {stage}: c");
        assert_converged(d, "public.t", case).await;
        assert_converged(d, "public.c", case).await;
    }
}

/// A plain 1-1 `t` over `public.a`, read by a plain 1-1 `c` (so every write
/// of `t` carries its prior image through the seam), built; then the source
/// is re-collated. Changes apply to both (the apply, its ledger probe and
/// the seam), both are checked with no divergence (the `self_check` page's
/// hash and compare), a rebuild in place removes an orphan (the sweep's
/// ledger page, orphan walk and seam), and a `RESUME` after deletes while
/// paused rebuilds `t` to the source.
async fn a_re_collated_key_applies_checks_rebuilds_and_resumes(r: Recollation<'_>) {
    let case = r.case;
    let mut columns: Vec<(&str, ValueType)> =
        r.names.iter().map(|n| (*n, ValueType::Text)).collect();
    columns.push(("v", int4()));
    let key_list = r.names.join(", ");
    // Each table's rows in one order, whatever its key's collations.
    let order: Vec<String> = r
        .names
        .iter()
        .map(|n| format!("{n} collate \"C\""))
        .collect();
    let order = order.join(", ");
    let mut d = Driver::start(
        &format!(
            "create table public.a ({}, v integer, primary key ({key_list})); \
             insert into public.a values {}",
            r.key_columns, r.source_rows
        ),
        &columns,
        &["TRANSFORM t FROM public.a SELECT a.v AS total"],
        &["public.a"],
    )
    .await;
    let trellis = trellis(&d).await;
    trellis
        .apply("TRANSFORM c FROM public.t SELECT t.total AS total")
        .await
        .unwrap_or_else(|e| panic!("{case}: define the reader: {e}"));
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    d.ctl
        .batch_execute(r.recollate)
        .await
        .unwrap_or_else(|e| panic!("{case}: {}: {e}", r.recollate));
    let tables = Tables {
        source: format!("select {key_list}, v from public.a order by {order}"),
        target: format!("select {key_list}, total from public.t order by {order}"),
        reader: format!("select {key_list}, total from public.c order by {order}"),
    };
    let rest: String = r.names[1..].iter().map(|n| format!(", {n}")).collect();
    d.user()
        .await
        .batch_execute(&format!(
            "update public.a set v = v + 100; delete from public.a where v = 102; \
             insert into public.a select id1 || 'n'{rest}, v + 10 from public.a"
        ))
        .await
        .unwrap_or_else(|e| panic!("{case}: changes to apply: {e}"));
    d.settle().await;
    tables.assert_rebuilt(&d, case, "applied").await;

    d.ctl
        .batch_execute(&format!(
            "insert into public.t ({key_list}, total) values {}",
            r.orphan
        ))
        .await
        .unwrap_or_else(|e| panic!("{case}: an orphan: {e}"));
    d.user()
        .await
        .batch_execute("update public.a set v = v + 1000 where v < 200")
        .await
        .unwrap_or_else(|e| panic!("{case}: changes behind the rebuild: {e}"));
    trellis
        .request_backfill("public.a")
        .await
        .unwrap_or_else(|e| panic!("{case}: request a rebuild: {e}"));
    work_to_idle(&d).await;
    let (status, error) = status_and_error(&d, "public.t").await;
    assert_eq!(
        (status.as_str(), error),
        ("live", None),
        "{case}: the rebuild finishes"
    );
    d.settle().await;
    tables.assert_rebuilt(&d, case, "rebuilt").await;

    trellis
        .apply("PAUSE TRANSFORM t")
        .await
        .unwrap_or_else(|e| panic!("{case}: pause: {e}"));
    d.user()
        .await
        .batch_execute("delete from public.a where v > 1100; update public.a set v = v + 1")
        .await
        .unwrap_or_else(|e| panic!("{case}: changes while paused: {e}"));
    d.settle().await;
    trellis
        .apply("RESUME TRANSFORM t")
        .await
        .unwrap_or_else(|e| panic!("{case}: resume: {e}"));
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .unwrap_or_else(|e| panic!("{case}: start the resumed build: {e}"));
    work_to_idle(&d).await;
    let (status, error) = status_and_error(&d, "public.t").await;
    assert_eq!(
        (status.as_str(), error),
        ("live", None),
        "{case}: the resume finishes"
    );
    d.settle().await;
    tables.assert_rebuilt(&d, case, "resumed").await;
}

/// One part of a key of one shared collation re-collated, so the source's
/// key mixes two non-default collations and the target's doesn't: before
/// #1537 every `self_check` page failed to hash the joined text.
#[tokio::test]
async fn a_composite_key_re_collated_to_mix_two_collations() {
    a_re_collated_key_applies_checks_rebuilds_and_resumes(Recollation::composite(
        "icu twice, then icu and POSIX",
        "und-x-icu",
        "und-x-icu",
        "alter table public.a alter column id2 type text collate \"POSIX\"",
    ))
    .await;
}

/// The other way round: the target's key mixes two collations and the
/// source's no longer does. Before #1537 a rebuild's sweep failed on every
/// attempt and paused the definition.
#[tokio::test]
async fn a_composite_key_re_collated_to_share_one_collation() {
    a_re_collated_key_applies_checks_rebuilds_and_resumes(Recollation::composite(
        "icu and POSIX, then icu twice",
        "und-x-icu",
        "POSIX",
        "alter table public.a alter column id2 type text collate \"und-x-icu\"",
    ))
    .await;
}

/// A part of the default collation beside a non-default one re-collated to a
/// second non-default one, and a non-default part re-collated to the
/// default: the key text's collation moves between the parts' and none.
#[tokio::test]
async fn a_composite_key_re_collated_from_or_to_the_default() {
    a_re_collated_key_applies_checks_rebuilds_and_resumes(Recollation::composite(
        "icu and default, then icu and POSIX",
        "und-x-icu",
        "default",
        "alter table public.a alter column id2 type text collate \"POSIX\"",
    ))
    .await;
    a_re_collated_key_applies_checks_rebuilds_and_resumes(Recollation::composite(
        "C and icu, then default and icu",
        "C",
        "und-x-icu",
        "alter table public.a alter column id1 type text collate \"default\"",
    ))
    .await;
}

/// The control: a single-column key re-collated keeps the column's own text
/// as its key, and every statement still resolves its collation.
#[tokio::test]
async fn a_single_column_key_re_collated() {
    a_re_collated_key_applies_checks_rebuilds_and_resumes(Recollation {
        case: "a single column, icu then POSIX",
        key_columns: "id1 text collate \"und-x-icu\"".to_string(),
        names: &["id1"],
        source_rows: "('a', 1), ('B', 2), ('c', 3), ('D', 4), ('aa', 5), ('Bb', 6)",
        orphan: "('zz', 99)",
        recollate: "alter table public.a alter column id1 type text collate \"POSIX\"",
    })
    .await;
}

// ---------------------------------------------------------------- 1-1 JOIN

const JOIN_DDL: &str = "create table public.a (id text collate \"und-x-icu\" primary key, \
                            k integer, r integer, s text); \
                        create table public.b (bid bigint primary key, k integer, img integer, \
                            k0 text); \
                        create unique index on public.b (k); \
                        create index on public.a (k); \
                        insert into public.a \
                            select (array['a', 'B', 'c', 'D'])[1 + i % 4] || i, i % 4 * 10, i, \
                                   's' || i \
                            from generate_series(1, 12) i; \
                        insert into public.b select i, i * 10, i * 100, 'k' || i \
                            from generate_series(1, 3) i";

const JOIN: &str = "TRANSFORM jl FROM public.a LEFT JOIN public.b ON a.k = b.k \
                    SELECT a.r + COALESCE(b.img, 0) AS total, b.k0 AS k0, a.s AS s";

async fn start_join() -> Driver {
    set_enabled(true);
    Driver::start(
        JOIN_DDL,
        &[
            ("id", ValueType::Text),
            ("k", int4()),
            ("r", int4()),
            ("s", ValueType::Text),
        ],
        &[JOIN],
        &["public.a", "public.b"],
    )
    .await
}

/// Asserts the join's rebuild finished and its target is the live join: the
/// orphan inserted into the target is gone.
async fn assert_the_join_is_rebuilt(d: &Driver, case: &str) {
    work_to_idle(d).await;
    let (status, error) = status_and_error(d, "public.jl").await;
    assert_eq!(
        (status.as_str(), error),
        ("live", None),
        "{case}: the rebuild finishes"
    );
    let actual = d
        .rows("select id, total, k0, s from public.jl order by id collate \"C\"")
        .await;
    let expected = d
        .rows(
            "select a.id, a.r + coalesce(b.img, 0), b.k0, a.s \
             from public.a a left join public.b b on a.k = b.k order by a.id collate \"C\"",
        )
        .await;
    assert_eq!(actual, expected, "{case}: the orphan is gone");
}

/// A rebuild in place of a 1-1 `JOIN` keyed by `und-x-icu` text: its sweep's
/// orphan pass removes a target row no live `R` entry backs.
#[tokio::test]
async fn a_rebuild_of_a_join_keyed_by_a_non_default_collation_removes_its_orphan() {
    let _serial = SERIAL.lock().await;
    let d = start_join().await;
    d.ctl
        .batch_execute("insert into public.jl (id, total, k0, s) values ('zz', 1, 'k', 's')")
        .await
        .expect("an orphan the source does not back");
    trellis(&d)
        .await
        .request_backfill("public.a")
        .await
        .expect("request a rebuild");
    assert_the_join_is_rebuilt(&d, "rebuild").await;
}

/// A `RESUME` of the same join after rows of `a` were deleted while paused:
/// `R` is recreated empty and the target kept, so the orphan pass removes
/// those rows' target rows.
#[tokio::test]
async fn a_resume_of_a_join_keyed_by_a_non_default_collation_removes_its_orphans() {
    let _serial = SERIAL.lock().await;
    let mut d = start_join().await;
    let trellis = trellis(&d).await;
    trellis.apply("PAUSE TRANSFORM jl").await.expect("pause");
    d.user()
        .await
        .batch_execute("delete from public.a where id in ('B1', 'c2')")
        .await
        .expect("delete while paused");
    let batch = d.seal().await;
    d.drain(batch, "delete while paused").await;
    trellis.apply("RESUME TRANSFORM jl").await.expect("resume");
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the resumed builds");
    assert_the_join_is_rebuilt(&d, "resume").await;
}

/// A 1-1 `JOIN` whose tables are each keyed by two parts of two non-default
/// collations (#1522): it builds, applies changes of both sides, is checked
/// with no divergence, and a rebuild and a `RESUME` remove its orphans.
#[tokio::test]
async fn a_join_keyed_by_parts_of_two_non_default_collations_builds_and_rebuilds() {
    let _serial = SERIAL.lock().await;
    set_enabled(true);
    let mut d = Driver::start(
        "create table public.a (id1 text collate \"und-x-icu\", id2 text collate \"POSIX\", \
             k integer, r integer, primary key (id1, id2)); \
         create table public.b (b1 text collate \"POSIX\", b2 text collate \"und-x-icu\", \
             k integer, img integer, k0 text, primary key (b1, b2)); \
         create unique index on public.b (k); \
         create index on public.a (k); \
         insert into public.a \
             select (array['a', 'B', 'c', 'D'])[1 + i % 4] || i, (array['x', 'Y'])[1 + i % 2], \
                    i % 4 * 10, i \
             from generate_series(1, 12) i; \
         insert into public.b select (array['p', 'Q'])[1 + i % 2], 'z' || i, i * 10, i * 100, \
             'k' || i from generate_series(1, 3) i",
        &[
            ("id1", ValueType::Text),
            ("id2", ValueType::Text),
            ("k", int4()),
            ("r", int4()),
        ],
        &[
            "TRANSFORM jm FROM public.a LEFT JOIN public.b ON a.k = b.k \
           SELECT a.r + COALESCE(b.img, 0) AS total, b.k0 AS k0",
        ],
        &["public.a", "public.b"],
    )
    .await;
    let actual = "select id1, id2, total, k0 from public.jm \
                  order by id1 collate \"C\", id2 collate \"C\"";
    let expected = "select a.id1, a.id2, a.r + coalesce(b.img, 0), b.k0 \
                    from public.a a left join public.b b on a.k = b.k \
                    order by a.id1 collate \"C\", a.id2 collate \"C\"";
    assert_eq!(d.rows(actual).await, d.rows(expected).await, "built");
    d.user()
        .await
        .batch_execute(
            "update public.a set k = 30, r = 0 where id1 = 'B1'; \
             delete from public.a where id1 = 'c2'; \
             insert into public.a values ('A99', 'q', 20, 99); \
             update public.b set img = 1 where b2 = 'z2'; \
             delete from public.b where b2 = 'z3'",
        )
        .await
        .expect("changes of both sides");
    d.settle().await;
    assert_eq!(d.rows(actual).await, d.rows(expected).await, "applied");
    assert_converged(&d, "public.jm", "join").await;
    d.ctl
        .batch_execute("insert into public.jm (id1, id2, total, k0) values ('zz', 'zz', 1, 'k')")
        .await
        .expect("an orphan the source does not back");
    let trellis = trellis(&d).await;
    trellis
        .request_backfill("public.a")
        .await
        .expect("request a rebuild");
    work_to_idle(&d).await;
    let (status, error) = status_and_error(&d, "public.jm").await;
    assert_eq!((status.as_str(), error), ("live", None), "rebuilt");
    assert_eq!(d.rows(actual).await, d.rows(expected).await, "rebuilt");
    trellis.apply("PAUSE TRANSFORM jm").await.expect("pause");
    d.user()
        .await
        .batch_execute("delete from public.a where id1 in ('D3', 'a4')")
        .await
        .expect("delete while paused");
    d.settle().await;
    trellis.apply("RESUME TRANSFORM jm").await.expect("resume");
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the resumed builds");
    work_to_idle(&d).await;
    let (status, error) = status_and_error(&d, "public.jm").await;
    assert_eq!((status.as_str(), error), ("live", None), "resumed");
    assert_eq!(d.rows(actual).await, d.rows(expected).await, "resumed");
    assert_converged(&d, "public.jm", "join, resumed").await;
}

/// A 1-1 `JOIN` whose tables are keyed `a (id1, id2)` and `b (b1, b2)` of
/// the collations `a_key` and `b_key` (`"<id1's>", "<id2's>"`), with
/// `recollate` run once it is built (#1537). It applies changes of both sides,
/// is checked with no divergence, and a rebuild and a `RESUME` remove its
/// orphans.
async fn a_join_re_collated_after_define(a_key: [&str; 2], b_key: [&str; 2], recollate: &str) {
    let _serial = SERIAL.lock().await;
    set_enabled(true);
    let mut d = Driver::start(
        &format!(
            "create table public.a (id1 text collate \"{}\", id2 text collate \"{}\", \
             k integer, r integer, primary key (id1, id2)); \
         create table public.b (b1 text collate \"{}\", b2 text collate \"{}\", \
             k integer, img integer, k0 text, primary key (b1, b2)); \
         create unique index on public.b (k); \
         create index on public.a (k); \
         insert into public.a \
             select (array['a', 'B', 'c', 'D'])[1 + i % 4] || i, (array['x', 'Y'])[1 + i % 2], \
                    i % 4 * 10, i \
             from generate_series(1, 12) i; \
         insert into public.b select (array['p', 'Q'])[1 + i % 2], 'z' || i, i * 10, i * 100, \
             'k' || i from generate_series(1, 3) i",
            a_key[0], a_key[1], b_key[0], b_key[1]
        ),
        &[
            ("id1", ValueType::Text),
            ("id2", ValueType::Text),
            ("k", int4()),
            ("r", int4()),
        ],
        &[
            "TRANSFORM jm FROM public.a LEFT JOIN public.b ON a.k = b.k \
           SELECT a.r + COALESCE(b.img, 0) AS total, b.k0 AS k0",
        ],
        &["public.a", "public.b"],
    )
    .await;
    d.ctl
        .batch_execute(recollate)
        .await
        .expect("re-collate a key part of each table");
    let actual = "select id1, id2, total, k0 from public.jm \
                  order by id1 collate \"C\", id2 collate \"C\"";
    let expected = "select a.id1, a.id2, a.r + coalesce(b.img, 0), b.k0 \
                    from public.a a left join public.b b on a.k = b.k \
                    order by a.id1 collate \"C\", a.id2 collate \"C\"";
    d.user()
        .await
        .batch_execute(
            "update public.a set k = 30, r = 0 where id1 = 'B1'; \
             delete from public.a where id1 = 'c2'; \
             insert into public.a values ('A99', 'q', 20, 99); \
             update public.b set img = 1 where b2 = 'z2'; \
             delete from public.b where b2 = 'z3'",
        )
        .await
        .expect("changes of both sides");
    d.settle().await;
    assert_eq!(d.rows(actual).await, d.rows(expected).await, "applied");
    assert_converged(&d, "public.jm", "join").await;
    d.ctl
        .batch_execute("insert into public.jm (id1, id2, total, k0) values ('zz', 'zz', 1, 'k')")
        .await
        .expect("an orphan the source does not back");
    let trellis = trellis(&d).await;
    trellis
        .request_backfill("public.a")
        .await
        .expect("request a rebuild");
    work_to_idle(&d).await;
    let (status, error) = status_and_error(&d, "public.jm").await;
    assert_eq!((status.as_str(), error), ("live", None), "rebuilt");
    assert_eq!(d.rows(actual).await, d.rows(expected).await, "rebuilt");
    assert_converged(&d, "public.jm", "join, rebuilt").await;
    trellis.apply("PAUSE TRANSFORM jm").await.expect("pause");
    d.user()
        .await
        .batch_execute(
            "delete from public.a where id1 in ('D3', 'a4'); \
             update public.b set img = 7 where b2 = 'z1'",
        )
        .await
        .expect("changes while paused");
    d.settle().await;
    trellis.apply("RESUME TRANSFORM jm").await.expect("resume");
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the resumed builds");
    work_to_idle(&d).await;
    let (status, error) = status_and_error(&d, "public.jm").await;
    assert_eq!((status.as_str(), error), ("live", None), "resumed");
    assert_eq!(d.rows(actual).await, d.rows(expected).await, "resumed");
    assert_converged(&d, "public.jm", "join, resumed").await;
}

/// `a`'s key from one shared collation to two, `b`'s from two to one: before
/// #1537 every `self_check` page failed to hash `a`'s key text.
#[tokio::test]
async fn a_join_whose_keys_are_re_collated_after_define() {
    a_join_re_collated_after_define(
        ["und-x-icu", "und-x-icu"],
        ["und-x-icu", "POSIX"],
        "alter table public.a alter column id2 type text collate \"POSIX\"; \
         alter table public.b alter column b2 type text collate \"und-x-icu\"",
    )
    .await;
}

/// The other way round: `a`'s key from two collations to one, `b`'s from one
/// to two.
#[tokio::test]
async fn a_join_whose_keys_are_re_collated_the_other_way_after_define() {
    a_join_re_collated_after_define(
        ["und-x-icu", "POSIX"],
        ["und-x-icu", "und-x-icu"],
        "alter table public.a alter column id2 type text collate \"und-x-icu\"; \
         alter table public.b alter column b2 type text collate \"POSIX\"",
    )
    .await;
}
