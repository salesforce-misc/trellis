//! 1-1 `JOIN` transforms whose sides are plain 1-1 targets (ADR-0021 "A side
//! that is another transform's target is read from its change feed", ADR-0002
//! "Multi-hop", #1487): the families S1–S9 of a seam-fed side.
//!
//! A target is never captured. Its writer stages each changed key through
//! the target-mutation seam, and a target a `JOIN` reads is on the seam's image
//! feed (`staging::target_mutations`, "The image feed"): a ring row like a
//! captured one, whose `lsn` is the writer's write token, read after its last
//! row lock and write. What differs from a captured side is what the upstream
//! does, so each family drives the upstream:
//!
//! - S1 writes to the upstream's source;
//! - S2 two upstream pages of one key out of order, and the `JOIN`'s pages of
//!   their seam rows out of order (I2 by write token);
//! - S3 a source primary-key update, a delete and an insert through the seam;
//! - S4 a `TRUNCATE` of the upstream's source, which the `JOIN` sees as
//!   deletes, key by key, under the fan-out cap;
//! - S5 the upstream's rebuild in place and its `RESUME`;
//! - S6 a `JOIN` defined while an upstream writer is in flight;
//! - S7 a paused upstream column the `JOIN` reads;
//! - S8 a row the upstream holds in quarantine;
//! - S9 `self_check` of the `JOIN` while the upstream lags.
//!
//! Every test defines four `JOIN`s over the same two upstreams, `ta` (of
//! `asrc`) as `a` and `tb` (of `bsrc`) as `b`: `LEFT` and `INNER`, each with
//! `a` on its primary key (`ta.id = tb.id`) and with `a` on a non-key column
//! (`ta.k = tb.id`, decision #1482). `b` is always on its primary key. After
//! each step the pipeline settles and every target equals its query written
//! over the captured tables, and over the chained tables as they are now, and
//! `self_check` finds nothing. Where the upstream's target deliberately
//! differs from its query (a paused column, a held row), the targets are held
//! to the chained tables alone.
//!
//! The `pin_…` tests are the pins of the 1-1 `JOIN`'s page plants
//! (`join1_*`, #1396–#1398) from `join_one_to_one_interleavings`, and of its
//! fan-out quarantine plants (#1513) from `join_one_to_one_quarantine`, with
//! `a` a seam-fed 1-1 target joined on a non-key column: each of those
//! plants fails at least one of them. S2 pins `seam_token_early`.
//!
//! Each test drives the pipeline by hand through `support/drain_driver.rs`;
//! nothing sleeps or polls for convergence; the waits are for states a test
//! forces (a backend queued on a lock, the self-check's convergence wait
//! started), as `Driver::wait_blocked_behind` is.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
use std::time::Duration;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::defs::join_one_to_one::{set_enabled, set_max_fanout};
use trellis::integer::IntWidth;
use trellis::staging::converge;
use trellis::staging::interleave::PausePoint;
use trellis::staging::quarantine;
use trellis::staging::self_check::{SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page};

/// The dev knob and the fan-out cap are process-wide, so these tests run one
/// at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

const SOURCES: &str = "create table public.asrc (id integer primary key, k integer, x integer); \
                       create table public.bsrc (id integer primary key, w integer, tag text)";

/// `asrc`'s rows: keys 10, 20 and 40 match a `bsrc` row, 30 and `NULL` none;
/// ids 1 to 3 match a `bsrc` row, 4 to 7 none.
const A_SEED: &str = "insert into public.asrc values \
                          (1, 10, 1), (2, 10, 2), (3, 20, 3), (4, 30, 4), (5, null, 5), \
                          (6, 20, 6), (7, 40, 7)";
const B_SEED: &str = "insert into public.bsrc values \
                          (1, 100, 'b1'), (2, 200, 'b2'), (3, 300, 'b3'), (9, 900, 'b9'), \
                          (10, 1000, 'b10'), (20, 2000, 'b20'), (40, 4000, 'b40')";

/// The `a` side's upstream. Its field `x + 1` fails on `x = 2147483647`,
/// which holds the row in quarantine (S8).
const TA: &str = "TRANSFORM ta FROM public.asrc SELECT k AS k, x + 1 AS x1";
/// The `b` side's upstream, likewise on `w`.
const TB: &str = "TRANSFORM tb FROM public.bsrc SELECT w + 1 AS w1, tag AS tag";

/// One join kind: `LEFT` keeps an `a` row with no `b` match, `INNER` drops it.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Left,
    Inner,
}

/// Which `ta` columns the `ON` reads: its primary key, or a non-key column.
#[derive(Debug, Clone, Copy, PartialEq)]
enum On {
    APk,
    ANonKey,
}

/// One of the four `JOIN`s every test defines.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Join {
    kind: Kind,
    on: On,
}

const JOINS: [Join; 4] = [
    Join {
        kind: Kind::Left,
        on: On::APk,
    },
    Join {
        kind: Kind::Inner,
        on: On::APk,
    },
    Join {
        kind: Kind::Left,
        on: On::ANonKey,
    },
    Join {
        kind: Kind::Inner,
        on: On::ANonKey,
    },
];

impl Join {
    fn name(self) -> &'static str {
        match (self.kind, self.on) {
            (Kind::Left, On::APk) => "jlp",
            (Kind::Inner, On::APk) => "jip",
            (Kind::Left, On::ANonKey) => "jln",
            (Kind::Inner, On::ANonKey) => "jin",
        }
    }

    fn target(self) -> String {
        format!("public.{}", self.name())
    }

    fn word(self) -> &'static str {
        match self.kind {
            Kind::Left => "LEFT",
            Kind::Inner => "INNER",
        }
    }

    /// The `ON` over `a`'s table aliased `a` and `b`'s aliased `b`.
    fn on(self, a: &str, b: &str) -> String {
        match self.on {
            On::APk => format!("{a}.id = {b}.id"),
            On::ANonKey => format!("{a}.k = {b}.id"),
        }
    }

    fn definition(self) -> String {
        format!(
            "TRANSFORM {} FROM public.ta {} JOIN public.tb ON {} \
             SELECT ta.x1 + COALESCE(tb.w1, 0) AS total, tb.tag AS tag",
            self.name(),
            self.word(),
            self.on("ta", "tb"),
        )
    }

    fn actual(self) -> String {
        format!("select id, total, tag from {} order by id", self.target())
    }

    /// The `JOIN` written over the captured tables, through both upstreams'
    /// fields.
    fn over_sources(self) -> String {
        format!(
            "select a.id, (a.x + 1) + coalesce(b.w + 1, 0), b.tag from public.asrc a \
             {} join public.bsrc b on {} order by a.id",
            self.word(),
            self.on("a", "b"),
        )
    }

    /// The `JOIN` written over the chained tables as they are.
    fn over_targets(self) -> String {
        format!(
            "select a.id, a.x1 + coalesce(b.w1, 0), b.tag from public.ta a \
             {} join public.tb b on {} order by a.id",
            self.word(),
            self.on("a", "b"),
        )
    }

    /// Whether the `JOIN` reads `ta`'s column `column`.
    fn reads_a(self, column: &str) -> bool {
        column == "x1" || (column == "k" && self.on == On::ANonKey)
    }
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it, and everything it propagates, to quiescence.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    d.settle().await;
}

/// Seals and drains `rounds` times: what a page staged downstream (the seam's
/// rows) applies in the next round. [`Driver::settle`] waits for nothing to
/// be pending, which a held key's parked work never is until its release.
async fn drain_rounds(d: &mut Driver, rounds: usize) {
    for _ in 0..rounds {
        let batch = d.seal().await;
        drain_charging(d, batch).await;
    }
}

/// Drains `batch`, again while a page fails on a key below the death
/// threshold: each failing drain charges the key once
/// (`quarantine::isolate_and_evict`), and the one that reaches the threshold
/// holds it and applies the rest.
async fn drain_charging(d: &Driver, batch: i64) {
    let mut last = None;
    for _ in 0..=quarantine::DEFAULT_DEATH_THRESHOLD {
        match d
            .drain_frozen(batch, "charging", &[])
            .await
            .finish_result()
            .await
        {
            Ok(_) => return,
            Err(err) => last = Some(err),
        }
    }
    panic!("batch {batch} never drained: {last:?}");
}

/// Runs every build to the end and drains, then does both again.
async fn settle_all(d: &mut Driver) {
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
}

/// The two upstreams live over the seeded sources, no `JOIN` defined.
async fn upstreams() -> Driver {
    set_enabled(true);
    Driver::start(
        &format!("{SOURCES}; {A_SEED}; {B_SEED}"),
        &[
            ("k", int4()),
            ("x", int4()),
            ("w", int4()),
            ("tag", ValueType::Text),
        ],
        &[TA, TB],
        &["public.asrc", "public.bsrc"],
    )
    .await
}

async fn define_joins(d: &Driver) {
    let columns = HashMap::from([
        ("total".to_string(), int4()),
        ("tag".to_string(), ValueType::Text),
    ]);
    for join in JOINS {
        trellis::defs::install_definition(d.pool(), &join.definition(), &columns, "public")
            .await
            .unwrap_or_else(|e| panic!("define {}: {e}", join.name()));
    }
}

/// [`upstreams`], with the four `JOIN`s defined over them, built and live.
async fn start() -> Driver {
    let mut d = upstreams().await;
    define_joins(&d).await;
    settle_all(&mut d).await;
    assert_exact(&mut d, "the build").await;
    d
}

/// Each definition's target name (bare) and status, by id.
async fn statuses(d: &Driver) -> Vec<String> {
    d.rows("select substr(target_table, 8), status::text from transform_definitions order by id")
        .await
}

/// Asserts every definition is `live`.
async fn assert_live(d: &Driver, when: &str) {
    let statuses = statuses(d).await;
    assert!(
        statuses.iter().all(|s| s.ends_with(",live)")),
        "{when}: {statuses:?}"
    );
}

/// A Trellis client over the driver's database.
async fn facade(d: &Driver) -> trellis::Trellis {
    trellis::Trellis::connect(
        trellis::config::Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

/// `name`'s reported status and the upstream columns it names as paused.
async fn reported(trellis: &trellis::Trellis, name: &str) -> (String, Vec<String>) {
    let status = trellis
        .status(name)
        .await
        .expect("status")
        .expect("the definition exists");
    (
        status.status.as_str().to_string(),
        status.paused_upstream_columns,
    )
}

/// Asserts every `JOIN`'s target equals its query over the chained tables as
/// they are, and `self_check` finds nothing. Settles nothing.
async fn assert_over_targets(d: &Driver, when: &str) {
    for join in JOINS {
        assert_eq!(
            d.rows(&join.actual()).await,
            d.rows(&join.over_targets()).await,
            "{when}: {} (left) differs from its query over ta and tb (right)",
            join.name()
        );
        assert_clean(d, join, when).await;
    }
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{when}: {deadlocks:?}");
}

/// Settles, and asserts every definition is live and every `JOIN` equals its
/// query over the captured tables and over the chained ones.
async fn assert_exact(d: &mut Driver, when: &str) {
    d.settle().await;
    assert_live(d, when).await;
    for join in JOINS {
        assert_eq!(
            d.rows(&join.actual()).await,
            d.rows(&join.over_sources()).await,
            "{when}: {} (left) differs from its query over the sources (right)",
            join.name()
        );
    }
    assert_over_targets(d, when).await;
}

/// Asserts `self_check` of `join` converges, walking every page.
async fn assert_clean(d: &Driver, join: Join, when: &str) {
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &join.target())
        .await
        .expect("self_check");
    assert!(
        diverging.is_empty(),
        "{when}: {} diverges at {diverging:?}",
        join.name()
    );
    let mut after = None;
    loop {
        let page = check_page(
            d.pool(),
            &join.target(),
            SelfCheckScope { after, limit: 3 },
            SelfCheckMode::Standard,
            Duration::from_secs(30),
        )
        .await
        .expect("a page of the check");
        assert_eq!(
            page.outcome,
            SelfCheckOutcome::Converged,
            "{when}: {}",
            join.name()
        );
        after = page.next_after;
        if after.is_none() {
            return;
        }
    }
}

// ------------------------------------------------------------------- S1

/// S1: writes to either upstream's source reach every `JOIN` through the
/// seam's images: inserts, updates of a field, of the non-key `ON` column
/// (a key move on `ta`), to and from `NULL`, deletes and re-inserts, and a
/// page mixing both sides.
#[tokio::test]
async fn s1_writes_to_the_upstream_sources_reach_the_join() {
    let _serial = SERIAL.lock().await;
    let mut d = start().await;
    for sql in [
        "insert into public.asrc values (8, 10, 8), (9, 9, 9)",
        "update public.asrc set x = 50 where id in (1, 4)",
        "update public.asrc set k = 20 where id = 1",
        "update public.asrc set k = null where id = 3; update public.asrc set k = 40 where id = 5",
        "delete from public.asrc where id in (2, 7)",
        "insert into public.asrc values (2, 40, 22)",
        "insert into public.bsrc values (4, 400, 'b4'), (30, 3000, 'b30')",
        "update public.bsrc set w = 111, tag = 'ten' where id = 10",
        "update public.bsrc set tag = null where id = 1",
        "delete from public.bsrc where id in (2, 20)",
        "insert into public.bsrc values (20, 2222, 'twenty')",
        "update public.asrc set k = 30, x = 31 where id = 6; \
         update public.bsrc set w = 333 where id = 30; \
         delete from public.bsrc where id = 3; \
         insert into public.asrc values (10, 30, 10)",
    ] {
        step(&mut d, sql).await;
        assert_exact(&mut d, sql).await;
    }
    // Several writes of one key on each side before one drain: the images
    // chain through each upstream write in write-token order.
    write(&d, "update public.asrc set k = 40 where id = 8").await;
    write(&d, "update public.asrc set k = 10, x = 80 where id = 8").await;
    write(&d, "update public.bsrc set w = 41 where id = 40").await;
    write(&d, "delete from public.bsrc where id = 40").await;
    write(&d, "insert into public.bsrc values (40, 42, 'again')").await;
    assert_exact(&mut d, "several writes of a key").await;
}

// ------------------------------------------------------------------- S2

/// S2, the upstream's ordering: two pages of one source key, the newer
/// drained first. The upstream refuses the older by its ledger's I2, so it
/// stages nothing, and the `JOIN`s follow the newer.
#[tokio::test]
async fn s2_an_older_upstream_page_after_a_newer_one_stages_nothing() {
    let _serial = SERIAL.lock().await;
    let mut d = start().await;
    for (older, newer) in [
        (
            "update public.asrc set x = 50, k = 40 where id = 1",
            "update public.asrc set x = 60, k = 20 where id = 1",
        ),
        (
            "update public.bsrc set w = 50 where id = 1; update public.bsrc set w = 51 where id = 10",
            "update public.bsrc set w = 60 where id = 1; delete from public.bsrc where id = 10",
        ),
    ] {
        write(&d, older).await;
        let older_batch = d.seal().await;
        write(&d, newer).await;
        let newer_batch = d.seal().await;
        d.drain(newer_batch, "newer").await;
        d.drain(older_batch, "older").await;
        assert_exact(&mut d, newer).await;
    }
}

/// S2, the write token: the newer upstream page of a key begins first and
/// stops before its entry lock, while the older page of the key applies and
/// commits; then the newer applies over it. The newer page commits second,
/// so its seam row must carry the later write token, though its transaction
/// began first: the token is read after the page's last row lock and write.
/// The `JOIN`s' pages of the two seam rows then run in either order, each
/// row in its own batch, and the older row is refused by its position.
///
/// Reading the token at the page's start (`seam_token_early`) gives the
/// newer row the earlier token, so a `JOIN` keeps the older state, which
/// the #1482 model showed only for seam-fed sides.
#[tokio::test]
async fn s2_seam_rows_of_one_key_order_by_their_writers_commits() {
    let _serial = SERIAL.lock().await;
    for (upstream, older, newer) in [
        (
            "public.ta",
            "update public.asrc set x = 50, k = 40 where id = 1",
            "update public.asrc set x = 60, k = 20 where id = 1",
        ),
        (
            "public.tb",
            "update public.bsrc set w = 50, tag = 'older' where id = 1; \
             update public.bsrc set w = 51 where id = 10",
            "update public.bsrc set w = 60, tag = 'newer' where id = 1; \
             update public.bsrc set w = 61 where id = 10",
        ),
    ] {
        for joins_in_order in [true, false] {
            let when = format!("{upstream}, the JOINs' pages in order: {joins_in_order}");
            let mut d = start().await;
            write(&d, older).await;
            let older_batch = d.seal().await;
            write(&d, newer).await;
            let newer_batch = d.seal().await;
            let mut newer_page = d
                .drain_frozen(
                    newer_batch,
                    "newer",
                    &[(PausePoint::AfterPlaceholders, upstream)],
                )
                .await;
            newer_page.reached(PausePoint::AfterPlaceholders).await;
            d.drain(older_batch, "older").await;
            // The older page's seam rows, in a batch of their own.
            let older_rows = d.seal().await;
            d.release(&mut newer_page, PausePoint::AfterPlaceholders)
                .await;
            newer_page.finish().await;
            let newer_rows = d.seal().await;
            if joins_in_order {
                d.drain(older_rows, "older rows").await;
                d.drain(newer_rows, "newer rows").await;
            } else {
                d.drain(newer_rows, "newer rows").await;
                d.drain(older_rows, "older rows").await;
            }
            assert_over_targets(&d, &when).await;
            assert_exact(&mut d, &when).await;
        }
    }
}

// ------------------------------------------------------------------- S3

/// S3: a source primary-key update is captured as a delete and an insert, so
/// the upstream deletes the old key's row and writes the new key's, and the
/// seam stages one row per key: the `JOIN`s see a delete and an insert. On
/// `ta` the row leaves one `a` key and arrives as another; on `tb` a `b` row
/// leaves the key its `a` rows join on and arrives on another.
#[tokio::test]
async fn s3_a_source_primary_key_update_reaches_the_join_as_a_delete_and_an_insert() {
    let _serial = SERIAL.lock().await;
    let mut d = start().await;
    for sql in [
        "update public.asrc set id = 11 where id = 1",
        "update public.asrc set id = 1 where id = 4",
        "update public.bsrc set id = 30 where id = 10",
        "update public.bsrc set id = 10 where id = 2",
        // Both sides in one statement each, and a key created and deleted in
        // one transaction, which stages nothing.
        "update public.asrc set id = id + 100; update public.bsrc set id = id + 100 where id < 5",
        "begin; insert into public.asrc values (50, 10, 1); \
         update public.asrc set id = 51 where id = 50; delete from public.asrc where id = 51; \
         commit",
    ] {
        step(&mut d, sql).await;
        assert_exact(&mut d, sql).await;
    }
}

// ------------------------------------------------------------------- S4

/// The number of `ta` rows a delete of every `tb` row fans out to in `join`:
/// the `a` rows it matches.
async fn matched_a_rows(d: &Driver, join: Join) -> u64 {
    d.ctl
        .query_one(
            &format!(
                "select count(*) from public.ta a join public.tb b on {}",
                join.on("a", "b")
            ),
            &[],
        )
        .await
        .expect("count the matched rows")
        .get::<_, i64>(0) as u64
}

/// The most `a` rows a delete of every `tb` row fans out to in any `JOIN`.
async fn widest_fan_out(d: &Driver) -> u64 {
    let mut widest = 0;
    for join in JOINS {
        widest = widest.max(matched_a_rows(d, join).await);
    }
    widest
}

/// S4: a `TRUNCATE` of an upstream's source clears its target row by row
/// through the seam (ADR-0019: a target is never truncated as a change of
/// its own), so a `JOIN` sees deletes, never a truncate. A `TRUNCATE bsrc`
/// starts no rebuild of the `JOIN`s (#1399's rule is for a truncate of `b`
/// itself): the upstream stages every key in one transaction, so a `JOIN`'s
/// page of them fans out over every `a` row they match at once, under the
/// cap as for any `b` change. With the cap at the widest such fan-out every
/// `JOIN` stays live, and refilled, and truncated and refilled before one
/// drain, they follow. One row under it, the truncate halts the widest
/// `JOIN` with the cap's reason: an upstream truncate costs a fan-out of
/// every matched `a` row, in one page.
#[tokio::test]
async fn s4_a_truncate_of_an_upstream_source_reaches_the_join_as_deletes() {
    let _serial = SERIAL.lock().await;
    let mut d = start().await;
    let widest = widest_fan_out(&d).await;
    set_max_fanout(widest);
    step(&mut d, "truncate public.bsrc").await;
    let rebuilds = d
        .rows("select count(*) from backfill_chunks where kind = 'plan' and not done")
        .await;
    assert_eq!(rebuilds, ["(0)"], "the truncate starts no rebuild");
    assert_exact(&mut d, "bsrc truncated").await;
    step(&mut d, B_SEED).await;
    assert_exact(&mut d, "bsrc refilled").await;
    step(&mut d, "truncate public.asrc").await;
    assert_exact(&mut d, "asrc truncated").await;
    write(&d, A_SEED).await;
    write(&d, "truncate public.asrc").await;
    write(&d, "insert into public.asrc values (1, 20, 1), (3, 10, 3)").await;
    write(&d, "truncate public.bsrc").await;
    write(
        &d,
        "insert into public.bsrc values (1, 7, 'seven'), (20, 8, 'eight')",
    )
    .await;
    let batch = d.seal().await;
    d.drain(batch, "truncated and refilled").await;
    assert_exact(&mut d, "truncated and refilled in one page").await;

    step(
        &mut d,
        &format!("delete from public.asrc; delete from public.bsrc; {A_SEED}; {B_SEED}"),
    )
    .await;
    assert_exact(&mut d, "reseeded").await;
    let widest = widest_fan_out(&d).await;
    set_max_fanout(widest - 1);
    step(&mut d, "truncate public.bsrc").await;
    set_max_fanout(0);
    let halted = d
        .rows(
            "select substr(d.target_table, 8), f.kind, \
                    f.error like '%TRELLIS_ONE_TO_ONE_JOIN_MAX_FANOUT%' \
             from transform_definitions d join capture_failures f on f.transform_id = d.id \
             where d.status = 'paused' order by d.id",
        )
        .await;
    // The `a`-on-pk `JOIN`s match 3 rows, the non-key ones 5.
    let expected: Vec<String> = JOINS
        .into_iter()
        .filter(|join| join.on == On::ANonKey)
        .map(|join| format!("({},halt,t)", join.name()))
        .collect();
    assert_eq!(
        halted, expected,
        "only the widest JOINs halt, on the cap: {halted:?}"
    );
    assert!(
        statuses(&d)
            .await
            .iter()
            .filter(|s| s.ends_with(",paused)"))
            .count()
            == expected.len(),
        "nothing else pauses"
    );
}

// ------------------------------------------------------------------- S5

/// Each `JOIN`'s build segment: a rebuild of a `JOIN` stamps a new one.
async fn join_build_segs(d: &Driver) -> Vec<String> {
    d.rows(
        "select substr(target_table, 8), build_seg from transform_definitions \
         where target_table in ('public.jlp', 'public.jip', 'public.jln', 'public.jin') \
         order by id",
    )
    .await
}

/// S5, a rebuild in place of either upstream (`request_backfill` of its
/// source): the upstream's Re-derive build writes through the seam, as images
/// of the rows its target held, so the `JOIN`s apply them like any change and
/// are not rebuilt (ADR-0002, "Multi-hop": a rebuild in place of a `live`
/// definition rebuilds nothing). Changes its snapshot saw before they drained
/// fold in.
#[tokio::test]
async fn s5_an_upstream_rebuild_in_place_reaches_the_join_through_the_seam() {
    let _serial = SERIAL.lock().await;
    let mut d = start().await;
    let trellis = facade(&d).await;
    for (table, upstream, sql) in [
        (
            "asrc",
            "ta",
            "update public.asrc set k = 40, x = 9 where id = 1; \
             insert into public.asrc values (8, 20, 8); delete from public.asrc where id = 3",
        ),
        (
            "bsrc",
            "tb",
            "insert into public.bsrc values (4, 400, 'b4'); delete from public.bsrc where id = 20; \
             update public.bsrc set w = 5 where id in (1, 10)",
        ),
    ] {
        write(&d, sql).await;
        let undrained = d.seal().await;
        let segs = join_build_segs(&d).await;
        trellis
            .request_backfill(table)
            .await
            .expect("request a rebuild");
        let rebuilding = statuses(&d).await;
        assert!(
            rebuilding
                .iter()
                .all(|s| s.ends_with(",live)") != (*s == format!("({upstream},backfilling)"))),
            "{table}: only the upstream rebuilds: {rebuilding:?}"
        );
        trellis::staging::build::settle_builds(d.pool()).await;
        assert_live(&d, table).await;
        d.drain(undrained, "undrained").await;
        assert_exact(&mut d, table).await;
        assert_eq!(
            join_build_segs(&d).await,
            segs,
            "{table}: no JOIN was rebuilt"
        );
    }
}

/// S5, a `RESUME` of either upstream after changes to its source while it
/// was paused (rows inserted, changed, moved onto and off a key, and
/// deleted, which only its rebuild removes from its target), with writes
/// during the rebuild. A plain 1-1 upstream's `RESUME` build re-derives its
/// rows under their locks and writes through the seam, as images of the rows
/// its target holds, so its readers' copies chain from them: the `JOIN`s
/// follow it with no rebuild of their own, as for a rebuild in place.
#[tokio::test]
async fn s5_an_upstream_resume_reaches_the_join() {
    let _serial = SERIAL.lock().await;
    let mut d = start().await;
    for (upstream, sql) in [
        (
            "ta",
            "update public.asrc set x = x + 10 where id in (1, 4); \
             update public.asrc set k = 40 where id = 2; update public.asrc set k = 30 where id = 3; \
             delete from public.asrc where id in (6, 7); insert into public.asrc values (8, 10, 8)",
        ),
        (
            "tb",
            "update public.bsrc set w = w + 10, tag = 'resumed' where id in (1, 10); \
             delete from public.bsrc where id in (2, 40); insert into public.bsrc values (4, 4, 'b4')",
        ),
    ] {
        d.ctl
            .batch_execute(&format!(
                "update transform_definitions set status = 'paused' \
                 where target_table = 'public.{upstream}'"
            ))
            .await
            .expect("pause the upstream");
        step(&mut d, sql).await;
        // The paused upstream's target is as it was; the `JOIN`s follow it.
        assert_over_targets(&d, &format!("{upstream} paused")).await;
        let segs = join_build_segs(&d).await;
        quarantine::resume_transform(d.pool(), upstream)
            .await
            .expect("resume the upstream");
        trellis::intake::harness::start_registered_builds(d.pool())
            .await
            .expect("dispatch the upstream's rebuild");
        let rebuilding = statuses(&d).await;
        assert!(
            rebuilding
                .iter()
                .all(|s| s.ends_with(",live)") != (*s == format!("({upstream},backfilling)"))),
            "{upstream}: only the upstream rebuilds: {rebuilding:?}"
        );
        write(
            &d,
            "insert into public.asrc values (20, 20, 20); update public.bsrc set w = 1 where id = 3",
        )
        .await;
        settle_all(&mut d).await;
        assert_exact(&mut d, &format!("{upstream} resumed")).await;
        assert_eq!(
            join_build_segs(&d).await,
            segs,
            "{upstream}: the JOINs follow the resume with no rebuild"
        );
        step(&mut d, "delete from public.asrc where id = 20").await;
        assert_exact(&mut d, &format!("after {upstream}'s resume")).await;
    }
}

// ------------------------------------------------------------------- S6

/// Claims one plan job, if any is claimable, and runs it.
async fn run_plan(d: &Driver) -> bool {
    let claimed = chunk_queue::claim_chunks_of(
        &**d.pool().get().await.expect("pool"),
        "test",
        1,
        &[chunk_queue::KIND_PLAN],
    )
    .await
    .expect("claim a plan job");
    let Some(chunk) = claimed.first() else {
        return false;
    };
    let options = trellis::staging::build::WorkerOptions {
        chunk_rows: 1_000,
        drain_batch_cap: usize::MAX,
        heartbeat_interval: Duration::from_secs(5),
        reclaim_ttl: Duration::from_secs(60),
    };
    trellis::staging::build::run_claimed(d.pool(), chunk, "test", &options).await;
    true
}

/// S6: the `JOIN`s are defined while an upstream page is in flight. The page
/// resolved its target's feed before they existed, so it stages nothing for
/// them, or, when the target already has a plain 1-1 reader, an image-less
/// `Recompute` per key, which a `JOIN` page takes as a Re-derive request. The
/// page stops after its target write, at the lock of a 1-1 `JOIN` over the
/// captured tables (`jx`) in the same batch, so the `JOIN`s' builds start
/// before it stages, and a seal moves its rows past their build segment.
/// Each plan waits on its seam fence until the page has ended, so the build
/// reads the page's write rather than missing it.
#[tokio::test]
async fn s6_a_join_defined_while_an_upstream_writer_is_in_flight_does_not_miss_its_write() {
    let _serial = SERIAL.lock().await;
    for (upstream, reader, sql) in [
        (
            "public.ta",
            None,
            "insert into public.asrc values (8, 10, 8); update public.asrc set k = 40 where id = 1",
        ),
        (
            "public.ta",
            Some("TRANSFORM tc FROM public.ta SELECT x1 AS x1"),
            "insert into public.asrc values (8, 10, 8); update public.asrc set k = 40 where id = 1",
        ),
        (
            "public.tb",
            None,
            "insert into public.bsrc values (4, 400, 'b4'); delete from public.bsrc where id = 10",
        ),
        (
            "public.tb",
            Some("TRANSFORM td FROM public.tb SELECT w1 AS w1"),
            "insert into public.bsrc values (4, 400, 'b4'); delete from public.bsrc where id = 10",
        ),
    ] {
        let when = format!("{upstream}, reader {reader:?}");
        let mut d = upstreams().await;
        let columns = HashMap::from([
            ("total".to_string(), int4()),
            ("x1".to_string(), int4()),
            ("w1".to_string(), int4()),
        ]);
        trellis::defs::install_definition(
            d.pool(),
            "TRANSFORM jx FROM public.asrc LEFT JOIN public.bsrc ON asrc.id = bsrc.id \
             SELECT asrc.x + COALESCE(bsrc.w, 0) AS total",
            &columns,
            "public",
        )
        .await
        .expect("define jx");
        if let Some(reader) = reader {
            trellis::defs::install_definition(d.pool(), reader, &columns, "public")
                .await
                .expect("define the 1-1 reader");
        }
        settle_all(&mut d).await;
        assert_live(&d, &when).await;

        write(&d, sql).await;
        let batch = d.seal().await;
        let mut page = d
            .drain_frozen(
                batch,
                "writer",
                &[(PausePoint::BeforeJoin1Lock, "public.jx")],
            )
            .await;
        page.reached(PausePoint::BeforeJoin1Lock).await;

        define_joins(&d).await;
        trellis::intake::harness::start_registered_builds(d.pool())
            .await
            .expect("start the JOINs' builds");
        // The page's rows land in a segment after the builds'.
        let between = d.seal().await;
        let mut deferred = 0;
        while run_plan(&d).await {
            deferred += 1;
        }
        assert_eq!(deferred, JOINS.len(), "{when}: a plan job per JOIN");
        assert_eq!(
            d.rows(
                "select done, fence_xid is not null from backfill_chunks where kind = 'plan' \
                 group by 1, 2"
            )
            .await,
            ["(f,t)"],
            "{when}: every plan defers on its fence"
        );

        d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
        page.finish().await;
        // The page's rows for its upstream's readers, in the active segment,
        // past every new JOIN's build segment.
        let slot: i16 = d
            .ctl
            .query_one("select ring_slot from segment_pointer", &[])
            .await
            .expect("the active slot")
            .get(0);
        let staged = d
            .rows(&format!(
                "select op, count(*) from seg_{slot} where src_table = '{upstream}' group by op"
            ))
            .await;
        let expected: Vec<String> = match reader {
            Some(_) => vec!["(recompute,2)".to_string()],
            None => vec![],
        };
        assert_eq!(staged, expected, "{when}: what the page staged");
        assert_eq!(
            d.rows(
                "select bool_and(d.build_seg < p.active_seq) from transform_definitions d, \
                 segment_pointer p where d.target_table in \
                 ('public.jlp', 'public.jip', 'public.jln', 'public.jin')"
            )
            .await,
            ["(t)"],
            "{when}: the page's rows land after the builds' segment"
        );
        d.drain(between, "between").await;
        d.ctl
            .batch_execute("update backfill_chunks set next_attempt_at = null")
            .await
            .expect("skip the plans' backoff");
        settle_all(&mut d).await;
        assert_exact(&mut d, &when).await;
        step(&mut d, "update public.asrc set k = 20 where id = 8").await;
        assert_exact(&mut d, &format!("{when}, written after")).await;
    }
}

// ------------------------------------------------------------------- S7

/// S7: a paused upstream column a `JOIN` reads, a field's on either side and
/// the non-key `ON` column of `ta`. Each `JOIN` reading it reports
/// `catching_up` and names it, and one that doesn't is unaffected. Meanwhile
/// the `JOIN`s apply over the frozen column and follow the chained tables as
/// they are (`self_check` excludes nothing of theirs and finds nothing). The
/// column's resume writes the upstream's rows through the seam, so the
/// `JOIN`s are exact again with no rebuild: for `ta.k`, every row whose key
/// moved meanwhile moves in the `JOIN`s that join on it.
#[tokio::test]
async fn s7_a_paused_upstream_column_the_join_reads() {
    let _serial = SERIAL.lock().await;
    for (column, sql) in [
        (
            "ta.x1",
            "update public.asrc set x = x + 50 where id in (1, 3, 7)",
        ),
        (
            "tb.w1",
            "update public.bsrc set w = w + 50 where id in (1, 10, 20)",
        ),
        (
            "tb.tag",
            "update public.bsrc set tag = 'new' where id in (2, 40)",
        ),
        (
            "ta.k",
            "update public.asrc set k = 40 where id = 1; update public.asrc set k = 10 where id = 4; \
             update public.asrc set k = null where id = 3; insert into public.asrc values (8, 20, 8)",
        ),
    ] {
        let mut d = start().await;
        let trellis = facade(&d).await;
        trellis
            .apply(&format!("PAUSE TRANSFORM {column}"))
            .await
            .expect("pause the column");
        let (table, name) = column.split_once('.').expect("a column");
        for join in JOINS {
            let reads = table == "tb" || join.reads_a(name);
            assert_eq!(
                reported(&trellis, join.name()).await,
                if reads {
                    ("catching_up".to_string(), vec![column.to_string()])
                } else {
                    ("live".to_string(), vec![])
                },
                "{column}: {}",
                join.name()
            );
        }
        step(&mut d, sql).await;
        assert_over_targets(&d, &format!("{column} paused")).await;
        let stale = JOINS
            .iter()
            .filter(|join| table == "tb" || join.reads_a(name));
        for join in stale {
            assert_ne!(
                d.rows(&join.actual()).await,
                d.rows(&join.over_sources()).await,
                "{column}: {} applies over the frozen column",
                join.name()
            );
        }
        trellis
            .apply(&format!("RESUME TRANSFORM {column}"))
            .await
            .expect("resume the column");
        settle_all(&mut d).await;
        for join in JOINS {
            assert_eq!(
                reported(&trellis, join.name()).await,
                ("live".to_string(), vec![]),
                "{column} resumed: {}",
                join.name()
            );
        }
        assert_exact(&mut d, &format!("{column} resumed")).await;
        step(&mut d, "update public.asrc set k = 20 where id = 2").await;
        assert_exact(&mut d, &format!("after {column}'s resume")).await;
    }
}

// ------------------------------------------------------------------- S8

/// The keys `upstream` holds in quarantine, sorted.
async fn held(d: &Driver, upstream: &str) -> Vec<String> {
    d.rows(&format!(
        "select p.key from poison p join transform_definitions d on d.id = p.transform_id \
         where d.target_table = 'public.{upstream}' order by p.key"
    ))
    .await
}

/// S8: rows an upstream holds in quarantine, a field of either side failing
/// on them: one an update of an existing row, whose target row stays as it
/// was, and one a new row, which its target never gets. The `JOIN`s follow
/// the chained tables as they are, and keep applying other changes of the
/// held rows' keys. Fixed at the source, the rows' changes are parked until
/// their release, which re-derives them through the seam; the `JOIN`s follow.
#[tokio::test]
async fn s8_a_row_the_upstream_holds_in_quarantine() {
    let _serial = SERIAL.lock().await;
    for (upstream, src, failing, held_keys, other, fix) in [
        (
            "ta",
            "public.asrc",
            "update public.asrc set x = 2147483647, k = 40 where id = 1; \
             insert into public.asrc values (8, 20, 2147483647)",
            ["(1)", "(8)"],
            "update public.asrc set x = 70 where id = 2; insert into public.asrc values (9, 10, 9)",
            "update public.asrc set x = 5 where id in (1, 8)",
        ),
        (
            "tb",
            "public.bsrc",
            "update public.bsrc set w = 2147483647, tag = 'held' where id = 10; \
             insert into public.bsrc values (30, 2147483647, 'b30')",
            ["(10)", "(30)"],
            "update public.bsrc set w = 71 where id = 1; update public.asrc set k = 30 where id = 2",
            "update public.bsrc set w = 5 where id in (10, 30)",
        ),
    ] {
        let mut d = start().await;
        write(&d, failing).await;
        drain_rounds(&mut d, 3).await;
        assert_eq!(
            held(&d, upstream).await,
            held_keys,
            "{upstream} holds its rows"
        );
        assert_live(&d, upstream).await;
        assert_over_targets(&d, &format!("{upstream} holds its rows")).await;
        write(&d, other).await;
        drain_rounds(&mut d, 3).await;
        assert_over_targets(&d, &format!("{upstream} holds its rows, others applied")).await;
        write(&d, fix).await;
        drain_rounds(&mut d, 3).await;
        assert_over_targets(&d, &format!("{upstream}'s fix parked")).await;
        for key in held_keys {
            let key = key.trim_start_matches('(').trim_end_matches(')');
            let released = quarantine::release_key(d.pool(), upstream, src, key)
                .await
                .expect("release the key");
            assert_eq!(released, 1, "{upstream}: release {key}");
        }
        settle_all(&mut d).await;
        assert!(held(&d, upstream).await.is_empty(), "{upstream}: released");
        assert_exact(&mut d, &format!("{upstream} released")).await;
    }
}

// ------------------------------------------------------------------- S9

/// One self-check page of `join`, waiting `timeout` for convergence.
async fn check(pool: &trellis::Pool, join: Join, timeout: Duration) -> SelfCheckOutcome {
    check_page(
        pool,
        &join.target(),
        SelfCheckScope {
            after: None,
            limit: 1000,
        },
        SelfCheckMode::Standard,
        timeout,
    )
    .await
    .expect("a page of the check")
    .outcome
}

/// Waits until a backend has started polling the convergence wait since
/// `since` (a `clock_timestamp()` the caller read before starting the
/// check): its watermark token is taken. A wait for a state the test has
/// forced, as `Driver::wait_blocked_behind` is.
async fn wait_polling<T>(d: &Driver, since: &str, checking: &tokio::task::JoinHandle<T>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let polling: bool = d
            .ctl
            .query_one(
                "select exists (select 1 from pg_stat_activity \
                 where datname = current_database() \
                   and query like '%select not (((select ring_slot from segment_pointer)%' \
                   and query_start > $1::text::timestamptz \
                   and pid <> pg_backend_pid())",
                &[&since],
            )
            .await
            .expect("read pg_stat_activity")
            .get(0);
        if polling || checking.is_finished() {
            return;
        }
        if std::time::Instant::now() >= deadline {
            let activity = d
                .rows(
                    "select pid, state, wait_event_type, wait_event, query_start::text, \
                            left(query, 300) \
                     from pg_stat_activity \
                     where pid <> pg_backend_pid() and datname = current_database()",
                )
                .await;
            panic!("no convergence wait started within 60 s (since {since}): {activity:#?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Runs a check of `join` that compares while the `JOIN` lags its upstreams:
/// `pre` is written before the check's watermark, so its wait needs it
/// applied; `post` after, and applied by its upstreams but not by the
/// `JOIN`s: its seam rows stay in the active segment until the check has
/// compared, and they carry `post`'s origin, after the watermark, so the wait
/// doesn't need them.
async fn check_while_the_join_lags(
    d: &mut Driver,
    join: Join,
    pre: &str,
    post: &str,
) -> SelfCheckOutcome {
    write(d, pre).await;
    let pre_batch = d.seal().await;
    let since: String = d
        .ctl
        .query_one("select clock_timestamp()::text", &[])
        .await
        .expect("read the clock")
        .get(0);
    let pool = d.pool().clone();
    let checking = tokio::spawn(async move { check(&pool, join, Duration::from_secs(60)).await });
    wait_polling(d, &since, &checking).await;
    assert!(
        !checking.is_finished(),
        "the check ended before its wait: {:?}",
        checking.await
    );
    write(d, post).await;
    let post_batch = d.seal().await;
    d.drain(pre_batch, "pre").await;
    let pre_rows = d.seal().await;
    // The upstreams apply `post`; its seam rows stay in the active segment.
    d.drain(post_batch, "post").await;
    d.drain(pre_rows, "pre rows").await;
    let outcome = checking.await.expect("the check");
    assert_ne!(
        d.rows(&join.actual()).await,
        d.rows(&join.over_targets()).await,
        "{}: the JOIN lags its upstreams as the check compares",
        join.name()
    );
    d.settle().await;
    outcome
}

/// S9: `self_check` of each `JOIN` while its upstreams lag. A watermark
/// through a source change waits for the change's seam rows, which carry the
/// source change's origin, not the upstream's later write token. With the
/// upstreams' source changes pending, its convergence wait times out as
/// `NotCaughtUp`, never a divergence. With the `JOIN` behind changes its
/// upstreams applied after the check's watermark, it compares against the
/// chained tables minus the seam rows it hasn't applied, and finds nothing.
/// A divergence planted in a target is found all the same.
#[tokio::test]
async fn s9_self_check_while_the_upstream_lags_reports_nothing_and_misses_nothing() {
    let _serial = SERIAL.lock().await;
    let mut d = start().await;
    write(
        &d,
        "update public.asrc set x = 77 where id = 3; update public.bsrc set w = 78 where id = 2",
    )
    .await;
    let token = converge::watermark_token(&d.ctl)
        .await
        .expect("a watermark");
    let source = d.seal().await;
    d.drain(source, "the upstreams").await;
    assert!(
        !converge::converged_through(&d.ctl, token)
            .await
            .expect("converged_through"),
        "the watermark waits for the JOINs' seam rows"
    );
    let seam_rows = d.seal().await;
    d.drain(seam_rows, "the seam rows").await;
    assert!(
        converge::converged_through(&d.ctl, token)
            .await
            .expect("converged_through"),
        "the seam rows applied, the watermark has converged"
    );
    assert_exact(&mut d, "the watermark").await;
    write(
        &d,
        "update public.asrc set k = 20, x = 70 where id = 1; delete from public.asrc where id = 2; \
         update public.bsrc set w = 71 where id = 20; delete from public.bsrc where id = 1",
    )
    .await;
    let pending = d.seal().await;
    for join in JOINS {
        assert_eq!(
            check(d.pool(), join, Duration::from_millis(300)).await,
            SelfCheckOutcome::NotCaughtUp,
            "{}: the upstreams lag their sources",
            join.name()
        );
    }
    d.drain(pending, "pending").await;
    assert_exact(&mut d, "drained").await;
    for (n, join) in JOINS.into_iter().enumerate() {
        let n = n as i32;
        let outcome = check_while_the_join_lags(
            &mut d,
            join,
            &format!(
                "update public.asrc set x = x + {n} where id = 3; \
                 update public.bsrc set tag = 'pre{n}' where id = 2"
            ),
            &format!(
                "update public.asrc set x = x + 1000, k = 40 where id = 6; \
                 update public.bsrc set w = w + {n} + 1 where id in (3, 20); \
                 insert into public.asrc values ({}, 20, 1)",
                100 + n
            ),
        )
        .await;
        assert_eq!(
            outcome,
            SelfCheckOutcome::Converged,
            "{}: the JOIN lags its upstreams",
            join.name()
        );
    }
    assert_exact(&mut d, "the lags drained").await;
    for join in JOINS {
        write(
            &d,
            &format!(
                "update {} set total = total + 1 where id = 3",
                join.target()
            ),
        )
        .await;
        let outcome = check_while_the_join_lags(
            &mut d,
            join,
            "update public.asrc set x = x + 1 where id = 4",
            "update public.bsrc set w = w + 1 where id = 40; update public.asrc set x = 1 where id = 6",
        )
        .await;
        assert!(
            matches!(&outcome, SelfCheckOutcome::Diverged(found) if found.len() == 1),
            "{}: the planted divergence is found: {outcome:?}",
            join.name()
        );
    }
}

// ------------------------------------------- the plants' pins, `a` seam-fed

// The ADR-0021 page plants (`join1_*` of #1396–#1398) are pinned by
// `join_one_to_one_interleavings` on captured sides. These are the same pins
// with `a` a 1-1 target of the captured `public.a`, joined on its non-key
// `k`, and `b` captured: a change of `a` is applied by the upstream's page
// and reaches the `JOIN` as its seam row, in a batch of its own, and the
// interleaving of the `JOIN`'s pages is the pin's. Each plant fails at least
// one of them. The last two port `join_one_to_one_quarantine`'s pins of the
// fan-out quarantine plants (#1513) the same way: the row the `JOIN` holds is
// held under `ta`'s key and re-derived from `ta`.

const PIN_DDL: &str = "create table public.a (id integer primary key, k integer, x integer, \
                           y text); \
                       create table public.b (id integer primary key, k integer, w integer, \
                           tag text); \
                       create unique index on public.b (k); \
                       create index on public.a (k)";
const PIN_A_ROWS: &str =
    "(1, 10, 1, 'one'), (2, 20, 2, 'two'), (3, 30, 3, 'three'), (4, null, 4, 'four')";
const PIN_B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty'), (3, 40, 400, 'forty')";

impl Kind {
    fn pin_name(self) -> &'static str {
        match self {
            Kind::Left => "jl",
            Kind::Inner => "ji",
        }
    }

    fn pin_target(self) -> String {
        format!("public.{}", self.pin_name())
    }

    fn pin_word(self) -> &'static str {
        match self {
            Kind::Left => "left",
            Kind::Inner => "inner",
        }
    }

    /// The pin's target rows as the live join of `a_table` and `b` computes
    /// them.
    fn pin_expected(self, a_table: &str) -> String {
        format!(
            "select a.id, a.x + coalesce(b.w, 0), b.tag, a.y from {a_table} a \
             {} join public.b b on a.k = b.k order by a.id",
            self.pin_word()
        )
    }

    fn pin_actual(self) -> String {
        format!(
            "select id, total, tag, y from {} order by id",
            self.pin_target()
        )
    }
}

const PIN_KINDS: [Kind; 2] = [Kind::Left, Kind::Inner];

/// `ta` over the seeded `a`, and the `kind` `JOIN` of `ta` and the seeded,
/// captured `b`, built and live.
async fn start_pin(kind: Kind) -> Driver {
    set_enabled(true);
    let mut d = Driver::start(
        &format!(
            "{PIN_DDL}; insert into public.a values {PIN_A_ROWS}; \
             insert into public.b values {PIN_B_ROWS}"
        ),
        &[
            ("id", int4()),
            ("k", int4()),
            ("x", int4()),
            ("y", ValueType::Text),
        ],
        &["TRANSFORM ta FROM public.a SELECT k AS k, x AS x, y AS y"],
        &["public.a"],
    )
    .await;
    trellis::defs::install_definition(
        d.pool(),
        &format!(
            "TRANSFORM {} FROM public.ta {} JOIN public.b ON ta.k = b.k \
             SELECT ta.x + COALESCE(b.w, 0) AS total, b.tag AS tag, ta.y AS y",
            kind.pin_name(),
            kind.pin_word()
        ),
        &HashMap::from([
            ("total".to_string(), int4()),
            ("tag".to_string(), ValueType::Text),
            ("y".to_string(), ValueType::Text),
        ]),
        "public",
    )
    .await
    .expect("define the JOIN");
    let catalog = trellis::capture::columns::load_catalog(&d.ctl)
        .await
        .expect("load the catalog");
    let spec = trellis::capture::columns::capture_spec(&d.ctl, &catalog, "public.b")
        .await
        .expect("capture spec");
    match trellis::capture::install::reconcile(
        &mut d.ctl,
        trellis::config::DEFAULT_SCHEMA,
        &spec,
        None,
    )
    .await
    .expect("capture b")
    {
        trellis::capture::install::Progress::Done(_) => {}
        trellis::capture::install::Progress::Waiting(wait) => panic!("capture waited: {wait}"),
    }
    settle_all(&mut d).await;
    assert_pin(&mut d, kind).await;
    d
}

/// Settles, and asserts `kind`'s target equals the live join over `a` and
/// over `ta`, `self_check` finds nothing and nothing deadlocked.
async fn assert_pin(d: &mut Driver, kind: Kind) {
    d.settle().await;
    let actual = d.rows(&kind.pin_actual()).await;
    assert_eq!(
        actual,
        d.rows(&kind.pin_expected("public.a")).await,
        "{kind:?} target (left) differs from the join over a (right)"
    );
    assert_eq!(
        actual,
        d.rows(&kind.pin_expected("public.ta")).await,
        "{kind:?} target (left) differs from the join over ta (right)"
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &kind.pin_target())
        .await
        .expect("self_check");
    assert!(diverging.is_empty(), "{kind:?} diverges at {diverging:?}");
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
}

/// Writes `sql` to the captured tables and drains it: the upstream applies
/// it, and its seam rows wait in the active segment.
async fn upstream(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "upstream").await;
}

/// [`upstream`], then seals the seam rows: the `JOIN`'s batch of the change.
async fn seal_a(d: &mut Driver, sql: &str) -> i64 {
    upstream(d, sql).await;
    d.seal().await
}

/// Two changes of `a`, each applied by its own upstream page, and the
/// `JOIN`'s batch of each one's seam rows, with Re-derive requests of
/// `rederive`'s `ta` keys staged into the second. Both source batches are
/// sealed before either drains, so the four-slot ring holds the two `JOIN`
/// batches undrained at once: a source batch retires only once its successor
/// has drained.
async fn seal_a_pair(d: &mut Driver, first: &str, second: &str, rederive: &[&str]) -> (i64, i64) {
    write(d, first).await;
    let first_source = d.seal().await;
    write(d, second).await;
    let second_source = d.seal().await;
    d.drain(first_source, "upstream").await;
    let first_batch = d.seal().await;
    d.drain(second_source, "upstream").await;
    d.stage_recomputes("public.ta", rederive).await;
    (first_batch, d.seal().await)
}

async fn pin_definition_id(d: &Driver, kind: Kind) -> i64 {
    d.ctl
        .query_one(
            "select id from transform_definitions where target_table = $1",
            &[&kind.pin_target()],
        )
        .await
        .expect("the definition")
        .get(0)
}

/// The label a page's `R` pause points carry for `kind`'s definition.
async fn pin_r_label(d: &Driver, kind: Kind) -> String {
    let id = pin_definition_id(d, kind).await;
    format!("{}.join1_{id}_r", trellis::config::DEFAULT_SCHEMA)
}

/// `kind`'s target `total` for `id`.
async fn pin_total(d: &Driver, kind: Kind, id: i32) -> Option<i32> {
    d.ctl
        .query_opt(
            &format!("select total from {} where id = $1", kind.pin_target()),
            &[&id],
        )
        .await
        .expect("read the target")
        .map(|row| row.get(0))
}

/// A connection that stands for a `b` page, as in
/// `join_one_to_one_interleavings`: holds `J[key]` `FOR UPDATE` and rewrites
/// what a `b` change on the key rewrites, uncommitted, until
/// [`PinBWriter::commit`].
struct PinBWriter {
    client: tokio_postgres::Client,
    pid: i32,
}

impl PinBWriter {
    async fn start(d: &Driver, kind: Kind, key: i32, w: i32) -> Self {
        let client = d.user().await;
        let pid: i32 = client
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        let id = pin_definition_id(d, kind).await;
        let schema = trellis::config::DEFAULT_SCHEMA;
        client
            .batch_execute(&format!(
                "begin; \
                 select 1 from {schema}.join1_{id}_j where k = {key} for update; \
                 update public.b set w = {w} where k = {key}; \
                 update {schema}.join1_{id}_m set w = {w} where k = {key}; \
                 update {} t set total = a.x + {w} from public.ta a \
                     where a.id = t.id and a.k = {key}",
                kind.pin_target()
            ))
            .await
            .expect("a b-side writer");
        PinBWriter { client, pid }
    }

    async fn commit(self) {
        self.client.batch_execute("commit").await.expect("commit");
    }
}

/// Waits until some backend queues on a lock `pid` holds (`true`), or until
/// `page` finishes without one having queued (`false`), as
/// `join_one_to_one_interleavings`'s helper of the same name does.
async fn queued_or_finished<T>(d: &Driver, pid: i32, page: &drain_driver::Running<T>) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let blocked: bool = d
            .ctl
            .query_one(
                "select exists (select 1 from pg_stat_activity \
                 where $1 = any(pg_blocking_pids(pid)))",
                &[&pid],
            )
            .await
            .expect("read pg_stat_activity")
            .get(0);
        if blocked {
            return true;
        }
        if page.is_finished() {
            return false;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the page neither queued behind pid {pid} nor finished within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Drains `first` frozen at its commit, then `second`, which must queue on a
/// lock the first holds (a `J` row); checks the target once both finish.
async fn pin_race(d: &mut Driver, kind: Kind, first: i64, second: i64) {
    let mut held = d
        .drain_frozen(
            first,
            "first",
            &[(PausePoint::BeforeCommit, &kind.pin_target())],
        )
        .await;
    let reached = held.reached(PausePoint::BeforeCommit).await;
    let queued = d.drain_frozen(second, "second", &[]).await;
    let blocked = queued_or_finished(d, reached.backend_pid, &queued).await;
    d.release(&mut held, PausePoint::BeforeCommit).await;
    held.finish().await;
    let result = queued.finish_result().await;
    assert!(
        blocked,
        "{kind:?}: the second page ran without queueing: {result:?}"
    );
    result.expect("the second page");
    assert_pin(d, kind).await;
}

/// One #1371 family with `a` seam-fed: the `JOIN`'s page of an `a` change
/// (`a_sql`, through the upstream) against a `b` page (`b_sql`) on a key they
/// share, either frozen at its commit while the other queues.
async fn pin_family(a_sql: &str, b_sql: &str) {
    for kind in PIN_KINDS {
        for a_first in [true, false] {
            let mut d = start_pin(kind).await;
            let a = seal_a(&mut d, a_sql).await;
            write(&d, b_sql).await;
            let b = d.seal().await;
            if a_first {
                pin_race(&mut d, kind, a, b).await;
            } else {
                pin_race(&mut d, kind, b, a).await;
            }
        }
    }
}

/// F1 (`join1_m_before_j`, `join1_no_j_lock`): an `a` insert on a key no `b`
/// row has ‖ the `b` insert of that key.
#[tokio::test]
async fn pin_f1_an_a_page_on_a_key_reads_m_only_after_the_b_page_on_it_commits() {
    let _serial = SERIAL.lock().await;
    pin_family(
        "insert into public.a values (5, 30, 5, 'five')",
        "insert into public.b values (4, 30, 300, 'thirty')",
    )
    .await;
}

/// F2: an `a` insert ‖ the `b` delete of its key.
#[tokio::test]
async fn pin_f2_an_a_insert_races_the_b_delete_of_its_key() {
    let _serial = SERIAL.lock().await;
    pin_family(
        "insert into public.a values (5, 10, 5, 'five')",
        "delete from public.b where id = 1",
    )
    .await;
}

/// F3 (`join1_no_j_lock`): an `a` move `10 → 20` ‖ a `b` update of `20`.
#[tokio::test]
async fn pin_f3_an_a_move_races_a_b_update_of_its_new_key() {
    let _serial = SERIAL.lock().await;
    pin_family(
        "update public.a set k = 20 where id = 1",
        "update public.b set w = 222 where k = 20",
    )
    .await;
}

/// F4: an `a` move `10 → 20` ‖ a `b` update of `10`, the key it leaves.
#[tokio::test]
async fn pin_f4_an_a_move_races_a_b_update_of_the_key_it_leaves() {
    let _serial = SERIAL.lock().await;
    pin_family(
        "update public.a set k = 20 where id = 1",
        "update public.b set w = 111 where k = 10",
    )
    .await;
}

/// F5: an `a` delete ‖ a `b` update of its key.
#[tokio::test]
async fn pin_f5_an_a_delete_races_a_b_update_of_its_key() {
    let _serial = SERIAL.lock().await;
    pin_family(
        "delete from public.a where id = 2",
        "update public.b set w = 222 where k = 20",
    )
    .await;
}

/// `join1_no_j_lock`: a page on a key a `b` writer holds queues on `J` and
/// reads `M` only once it has the lock.
#[tokio::test]
async fn pin_an_a_page_holds_its_j_row_until_it_commits() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        let batch = seal_a(&mut d, "insert into public.a values (5, 10, 5, 'five')").await;
        let b = PinBWriter::start(&d, kind, 10, 999).await;
        let page = d.drain_frozen(batch, "page", &[]).await;
        let blocked = queued_or_finished(&d, b.pid, &page).await;
        b.commit().await;
        let result = page.finish_result().await;
        assert!(blocked, "{kind:?}: the page ran through: {result:?}");
        result.expect("the page");
        assert_pin(&mut d, kind).await;
    }
}

/// `join1_old_key_unlocked`: a page whose row moves off a key locks the key
/// it leaves, the one `R` holds.
#[tokio::test]
async fn pin_a_row_moving_off_a_key_locks_the_key_it_leaves() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        let batch = seal_a(&mut d, "update public.a set k = 20 where id = 1").await;
        let b = PinBWriter::start(&d, kind, 10, 555).await;
        let page = d.drain_frozen(batch, "page", &[]).await;
        let blocked = queued_or_finished(&d, b.pid, &page).await;
        b.commit().await;
        let result = page.finish_result().await;
        assert!(blocked, "{kind:?}: the page ran through: {result:?}");
        result.expect("the page");
        assert_pin(&mut d, kind).await;
    }
}

/// `join1_no_recheck`: the key an `R` entry holds moved after the page read
/// its key set, so the page rolls back and its retry locks the new key too,
/// queueing on the `b` writer that holds it.
#[tokio::test]
async fn pin_a_key_that_moved_under_the_page_rolls_it_back_and_the_retry_locks_both() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        // Row 2 is on key 20. Two changes: 20 -> 40, then 40 -> 10.
        let (first, second) = seal_a_pair(
            &mut d,
            "update public.a set k = 40 where id = 2",
            "update public.a set k = 10 where id = 2",
            &[],
        )
        .await;
        let moves = trellis::dev::interleave::join_key_moves();
        let mut late = d
            .drain_frozen(
                second,
                "late",
                &[(PausePoint::BeforeJoin1Lock, &kind.pin_target())],
            )
            .await;
        late.reached(PausePoint::BeforeJoin1Lock).await;
        d.drain(first, "early").await;
        let b = PinBWriter::start(&d, kind, 40, 777).await;
        d.release(&mut late, PausePoint::BeforeJoin1Lock).await;
        d.wait_blocked_behind(b.pid).await;
        assert_eq!(
            trellis::dev::interleave::join_key_moves(),
            moves + 1,
            "{kind:?}: the page rolled back once on the moved key"
        );
        b.commit().await;
        late.finish().await;
        assert_pin(&mut d, kind).await;
    }
}

/// `join1_vis_only`: a seam row older than the entry's applied position is
/// refused, though the entry has no basis.
#[tokio::test]
async fn pin_an_older_apply_after_a_newer_one_is_refused() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        let (older, newer) = seal_a_pair(
            &mut d,
            "update public.a set x = 11 where id = 1",
            "update public.a set x = 12 where id = 1",
            &[],
        )
        .await;
        d.drain(newer, "newer").await;
        d.drain(older, "older").await;
        assert_eq!(pin_total(&d, kind, 1).await, Some(12 + 100), "{kind:?}");
        assert_pin(&mut d, kind).await;
    }
}

/// `join1_lsn_only` (R17): a Re-derive request of `ta`'s row (a seam
/// `Recompute`) folds with its newer seam row into a Re-derive, which reads
/// the row; the older seam row then arrives, and the entry's basis saw its
/// writer, so it is refused.
#[tokio::test]
async fn pin_r17_a_stale_apply_after_a_rederive_is_refused() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        let (older, rederive) = seal_a_pair(
            &mut d,
            "update public.a set x = 11 where id = 1",
            "update public.a set x = 12 where id = 1",
            &["1"],
        )
        .await;
        d.drain(rederive, "rederive").await;
        assert_eq!(pin_total(&d, kind, 1).await, Some(12 + 100));
        d.drain(older, "older").await;
        assert_eq!(pin_total(&d, kind, 1).await, Some(12 + 100), "{kind:?}");
        assert_pin(&mut d, kind).await;
    }
}

/// `join1_no_entry_lock`: two pages on a `NULL`-keyed row take no `J` row,
/// so the entry lock orders them.
#[tokio::test]
async fn pin_n_two_pages_on_a_null_keyed_row_queue_on_its_entry() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        let label = pin_r_label(&d, kind).await;
        let (first, second) = seal_a_pair(
            &mut d,
            "update public.a set x = 41 where id = 4",
            "update public.a set x = 42 where id = 4",
            &[],
        )
        .await;
        let mut held = d
            .drain_frozen(
                first,
                "first",
                &[(PausePoint::AfterJoin1REntryLock, &label)],
            )
            .await;
        let reached = held.reached(PausePoint::AfterJoin1REntryLock).await;
        let queued = d.drain_frozen(second, "second", &[]).await;
        d.wait_blocked_behind(reached.backend_pid).await;
        assert!(!queued.is_finished());
        d.release(&mut held, PausePoint::AfterJoin1REntryLock).await;
        held.finish().await;
        queued.finish().await;
        assert_pin(&mut d, kind).await;
    }
}

/// `join1_no_placeholder`: two pages on a row no `R` entry exists for yet;
/// the newer page's placeholder insert is uncommitted, so the older queues on
/// it rather than overwriting it.
#[tokio::test]
async fn pin_two_pages_on_a_new_row_queue_on_the_placeholder() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        let label = pin_r_label(&d, kind).await;
        let (older, newer) = seal_a_pair(
            &mut d,
            "insert into public.a values (9, 10, 9, 'nine')",
            "update public.a set x = 99 where id = 9",
            &[],
        )
        .await;
        let mut held = d
            .drain_frozen(newer, "newer", &[(PausePoint::AfterPlaceholders, &label)])
            .await;
        let reached = held.reached(PausePoint::AfterPlaceholders).await;
        let queued = d.drain_frozen(older, "older", &[]).await;
        d.wait_blocked_behind(reached.backend_pid).await;
        d.release(&mut held, PausePoint::AfterPlaceholders).await;
        held.finish().await;
        queued.finish().await;
        assert_eq!(pin_total(&d, kind, 9).await, Some(99 + 100), "{kind:?}");
        assert_pin(&mut d, kind).await;
    }
}

/// `join1_snapshot_after_read`: the upstream writes `ta`'s row after a
/// Re-derive's read statement, so its writer is not in the entry's basis,
/// and its seam row, drained after the Re-derive, applies.
#[tokio::test]
async fn pin_a_change_between_a_rederives_read_and_its_snapshot_is_applied() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        d.stage_recomputes("public.ta", &["1"]).await;
        let rederive = d.seal().await;
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive",
                &[(PausePoint::AfterRederiveRead, &kind.pin_target())],
            )
            .await;
        page.reached(PausePoint::AfterRederiveRead).await;
        upstream(&mut d, "update public.a set x = 500 where id = 1").await;
        d.release(&mut page, PausePoint::AfterRederiveRead).await;
        page.finish().await;
        let change = d.seal().await;
        d.drain(change, "change").await;
        assert_eq!(pin_total(&d, kind, 1).await, Some(500 + 100), "{kind:?}");
        assert_pin(&mut d, kind).await;
    }
}

/// `join1_rd_no_key` (R19): `ta`'s row moves onto key 40 after a Re-derive
/// page's unlocked read of it, while a `b` writer on 40 holds `J[40]`
/// uncommitted. The retry queues on `J[40]` and reads the writer's `M`
/// entry. (The pin's `b` page inserts a row on its key; here a writer of an
/// existing one stands for it, so the ring holds one batch fewer.)
#[tokio::test]
async fn pin_r19_an_a_rederive_whose_row_moved_onto_a_held_key_retries() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        d.stage_recomputes("public.ta", &["1"]).await;
        let rederive = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive a",
                &[(PausePoint::BeforeJoin1Lock, &kind.pin_target())],
            )
            .await;
        page.reached(PausePoint::BeforeJoin1Lock).await;
        upstream(&mut d, "update public.a set k = 40 where id = 1").await;
        let b = PinBWriter::start(&d, kind, 40, 444).await;
        d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
        let blocked = queued_or_finished(&d, b.pid, &page).await;
        b.commit().await;
        let result = page.finish_result().await;
        assert!(blocked, "{kind:?}: the Re-derive ran through: {result:?}");
        result.expect("the Re-derive page");
        assert_eq!(
            trellis::dev::interleave::join_key_moves(),
            moves + 1,
            "{kind:?}: the Re-derive's read found a key it hadn't locked"
        );
        assert_eq!(pin_total(&d, kind, 1).await, Some(1 + 444), "{kind:?}");
        assert_pin(&mut d, kind).await;
    }
}

/// `join1_brd_no_key` (R18): a `b` row moves onto key 30 after a `b`
/// Re-derive page's unlocked read of it, while the `JOIN`'s page of an `a`
/// insert on 30 (`ta`'s seam row) holds `J[30]` uncommitted. The Re-derive
/// retries and queues on `J[30]`, and its fan-out reaches the new row.
#[tokio::test]
async fn pin_r18_a_b_rederive_whose_row_moved_onto_a_held_key_retries() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        let insert = seal_a(&mut d, "insert into public.a values (5, 30, 5, 'five')").await;
        d.stage_recomputes("public.b", &["1"]).await;
        let rederive = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive b",
                &[(PausePoint::BeforeJoin1Lock, &kind.pin_target())],
            )
            .await;
        page.reached(PausePoint::BeforeJoin1Lock).await;
        // Captured, left for the settle to drain: the Re-derive's basis sees
        // it, so its Apply is refused.
        write(&d, "update public.b set k = 30 where id = 1").await;
        let mut held = d
            .drain_frozen(
                insert,
                "a",
                &[(PausePoint::BeforeCommit, &kind.pin_target())],
            )
            .await;
        let reached = held.reached(PausePoint::BeforeCommit).await;
        d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
        let blocked = queued_or_finished(&d, reached.backend_pid, &page).await;
        d.release(&mut held, PausePoint::BeforeCommit).await;
        held.finish().await;
        let result = page.finish_result().await;
        assert!(blocked, "{kind:?}: the Re-derive ran through: {result:?}");
        result.expect("the Re-derive page");
        assert_eq!(
            trellis::dev::interleave::join_key_moves(),
            moves + 1,
            "{kind:?}: the Re-derive's read found a key it hadn't locked"
        );
        assert_pin(&mut d, kind).await;
    }
}

/// `ta`'s row 23 on key 10, with an `x` that overflows `total` once `b`'s
/// `w` on 10 reaches 1000, and a `b` change that takes it there: the
/// `JOIN`'s fan-out holds the row under `ta`'s key.
async fn hold_a_seam_fed_row(d: &mut Driver, kind: Kind) {
    upstream(d, "insert into public.a values (23, 10, 2147483000, 'big')").await;
    d.settle().await;
    write(d, "update public.b set w = 1000 where id = 1").await;
    let fan_out = d.seal().await;
    d.drain(fan_out, "fan-out").await;
    assert_eq!(
        d.rows("select src_table, key from poison").await,
        ["(public.ta,23)"],
        "{kind:?}: the fan-out holds the row"
    );
}

/// Releases `ta`'s key 23 for `kind`.
async fn release_23(d: &Driver, kind: Kind) {
    let released = quarantine::release_key(d.pool(), kind.pin_name(), "public.ta", "23")
        .await
        .expect("release the key");
    assert_eq!(released, 1, "{kind:?}: 23 had its parked work");
}

/// `join1_fan_out_held_row`: a later fan-out of `b` leaves the row the
/// `JOIN` holds of its seam-fed `a` as it was, though its fields would
/// compute, until the release re-derives it from `ta`.
#[tokio::test]
async fn pin_a_fan_out_leaves_a_held_row_of_a_seam_fed_a_as_it_was() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        hold_a_seam_fed_row(&mut d, kind).await;
        write(&d, "update public.b set w = 0, tag = 'zero' where id = 1").await;
        let fan_out = d.seal().await;
        d.drain(fan_out, "a second fan-out").await;
        assert_eq!(
            d.rows(&format!(
                "select total, tag from {} where id = 23",
                kind.pin_target()
            ))
            .await,
            [format!("({},ten)", 2_147_483_000 + 100)],
            "{kind:?}: the held row isn't rewritten"
        );
        release_23(&d, kind).await;
        assert_pin(&mut d, kind).await;
        assert!(d.rows("select key from poison").await.is_empty());
    }
}

/// `join1_release_no_rederive`: the held row's change through the seam is
/// parked; its release while a `b` change on its key waits in an earlier
/// batch re-derives the row's `R` entry from `ta` in its own transaction,
/// so the pending fan-out doesn't hold it again.
#[tokio::test]
async fn pin_a_release_of_a_seam_fed_row_before_a_pending_fan_out_stays_released() {
    let _serial = SERIAL.lock().await;
    for kind in PIN_KINDS {
        let mut d = start_pin(kind).await;
        hold_a_seam_fed_row(&mut d, kind).await;
        let change = seal_a(&mut d, "update public.a set x = 23 where id = 23").await;
        d.drain(change, "the held row's change").await;
        write(&d, "update public.b set w = 1001 where id = 1").await;
        let pending = d.seal().await;
        release_23(&d, kind).await;
        d.drain(pending, "pending").await;
        assert!(
            d.rows("select key from poison").await.is_empty(),
            "{kind:?}: the pending fan-out held the released row again"
        );
        assert_pin(&mut d, kind).await;
    }
}
