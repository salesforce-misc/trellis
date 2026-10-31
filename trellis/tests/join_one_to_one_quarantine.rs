//! Quarantine on 1-1 `JOIN` targets (ADR-0021, #1513): the rows of `a` a
//! `b` change's fan-out fails on, the fan-out's skip of held rows, the
//! release that re-derives them, a held key of `b`, and the column fuse's
//! fence over both tables.
//!
//! Every test turns the dev knob on before its driver starts, seeds `a` and
//! `b`, defines the transform and drives sealed batches by hand
//! (`support/drain_driver.rs`). Nothing sleeps or polls for convergence but
//! the page a pause must queue behind.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;
use trellis::staging::quarantine;

const DDL: &str = "create table public.a (id integer primary key, k integer, x integer, \
                       y text); \
                   create table public.b (id integer primary key, k integer, w integer, \
                       tag text); \
                   create unique index on public.b (k); \
                   create index on public.a (k)";

/// The dev knob and the process-wide test hooks are shared, so these tests
/// run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

fn int8() -> ValueType {
    ValueType::Integer(IntWidth::Int8)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Left,
    Inner,
}

const KINDS: [Kind; 2] = [Kind::Left, Kind::Inner];

impl Kind {
    /// The bare target: the transform's name.
    fn name(self) -> &'static str {
        match self {
            Kind::Left => "jl",
            Kind::Inner => "ji",
        }
    }

    fn target(self) -> String {
        format!("public.{}", self.name())
    }

    fn definition(self) -> String {
        let word = match self {
            Kind::Left => "LEFT JOIN",
            Kind::Inner => "INNER JOIN",
        };
        format!(
            "TRANSFORM {} FROM public.a {word} public.b ON a.k = b.k \
             SELECT a.x + COALESCE(b.w, 0) AS total, b.tag AS tag, a.y AS y",
            self.name()
        )
    }

    /// The target's rows as the live join computes them, but `held`.
    fn expected(self, held: &[i32]) -> String {
        let join = match self {
            Kind::Left => "left join",
            Kind::Inner => "join",
        };
        format!(
            "select a.id, a.x + coalesce(b.w, 0) as total, b.tag, a.y \
             from public.a {join} public.b on a.k = b.k \
             where a.id <> all('{{{}}}'::int[]) order by a.id",
            held.iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    fn actual(self, held: &[i32]) -> String {
        format!(
            "select id, total, tag, y from {} where id <> all('{{{}}}'::int[]) order by id",
            self.target(),
            held.iter()
                .map(i32::to_string)
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

/// A database with `a` seeded with `a_rows` `(id, k, x, y)` and `b` with
/// `b_rows` `(id, k, w, tag)`, `kind` defined and built, both captured.
async fn start(kind: Kind, a_rows: &str, b_rows: &str) -> Driver {
    set_enabled(true);
    let ddl = format!(
        "{DDL}; insert into public.a values {a_rows}; insert into public.b values {b_rows}"
    );
    Driver::start(
        &ddl,
        &[
            ("id", int4()),
            ("k", int4()),
            ("x", int4()),
            ("y", ValueType::Text),
        ],
        &[&kind.definition()],
        &["public.a", "public.b"],
    )
    .await
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it as one sealed batch.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

async fn definition_id(d: &Driver, target: &str) -> i64 {
    d.ctl
        .query_one(
            "select id from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("the definition")
        .get(0)
}

async fn status(d: &Driver, target: &str) -> String {
    d.ctl
        .query_one(
            "select status::text from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("the definition")
        .get(0)
}

async fn total(d: &Driver, kind: Kind, id: i32) -> Option<i32> {
    d.ctl
        .query_opt(
            &format!("select total from {} where id = $1", kind.target()),
            &[&id],
        )
        .await
        .expect("read the target")
        .map(|row| row.get(0))
}

/// Asserts `kind`'s target equals the live join on every row but `held`, and
/// `M` holds exactly `b`'s live rows.
async fn assert_join_but(d: &Driver, kind: Kind, held: &[i32], step: &str) {
    let actual = d.rows(&kind.actual(held)).await;
    let expected = d.rows(&kind.expected(held)).await;
    assert_eq!(
        actual, expected,
        "{kind:?} target (left) differs from the live join (right) after {step}"
    );
    let id = definition_id(d, &kind.target()).await;
    let m = format!("{DEFAULT_SCHEMA}.join1_{id}_m");
    let copied = d
        .rows(&format!(
            "select __from_key, k, w, tag from {m} where not __tombstone order by 1"
        ))
        .await;
    let live = d
        .rows("select id::text, k, w, tag from public.b order by id::text")
        .await;
    assert_eq!(
        copied, live,
        "{kind:?} M (left) differs from b (right) after {step}"
    );
}

/// Settles the pipeline and asserts the target is exactly the live join, with
/// nothing held.
async fn assert_exact(d: &mut Driver, kind: Kind, step: &str) {
    d.settle().await;
    assert_join_but(d, kind, &[], step).await;
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &kind.target())
        .await
        .expect("self_check");
    assert!(
        diverging.is_empty(),
        "{kind:?} diverges at {diverging:?} after {step}"
    );
    assert!(
        d.rows("select key from poison").await.is_empty(),
        "after {step}"
    );
    assert!(
        d.rows("select key from poison_held").await.is_empty(),
        "after {step}"
    );
    assert_eq!(status(d, &kind.target()).await, "live", "after {step}");
}

const A_ROWS: &str = "(1, 10, 1, 'one'), (2, 20, 2, 'two'), (3, 30, 3, 'three')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty')";

/// `a` rows 5 to 40 on key 10, those in `big` with an `x` that overflows `x
/// + w` once `w` reaches 1000.
async fn seed_key_10(d: &mut Driver, big: &[i32]) {
    let rows: Vec<String> = (5..=40)
        .map(|id| {
            let x = if big.contains(&id) { 2_147_483_000 } else { id };
            format!("({id}, 10, {x}, 'r')")
        })
        .collect();
    step(
        d,
        &format!("insert into public.a values {}", rows.join(", ")),
    )
    .await;
}

/// Releases `key` of `table` for `kind` and drains the Re-derive it stages.
async fn release(d: &mut Driver, kind: Kind, table: &str, key: &str) {
    let held = quarantine::release_key(d.pool(), kind.name(), table, key)
        .await
        .expect("release the key");
    assert_eq!(held, 1, "{key} had its parked work");
    let batch = d.seal().await;
    d.drain(batch, "release").await;
}

/// A field that fails on one row of a `b` change's fan-out holds that row of
/// `a` in quarantine and the change applies to every other row: the
/// fan-out's batches are bisected under savepoints down to the row, the
/// per-field probe names `total`, and the page commits with `M` advanced and
/// the transform live. The row is held under `a`'s key with a parked
/// Re-derive and counts toward `total`'s column fuse; its own change while
/// held is parked too, and its release re-derives it.
#[tokio::test]
async fn a_field_failing_on_a_fan_out_row_holds_that_row_and_the_b_change_applies() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind, A_ROWS, B_ROWS).await;
        seed_key_10(&mut d, &[23]).await;
        step(&mut d, "update public.b set w = 1000 where id = 1").await;
        assert_eq!(status(&d, &kind.target()).await, "live", "{kind:?}");
        assert_join_but(&d, kind, &[23], "the fan-out").await;
        assert_eq!(
            total(&d, kind, 23).await,
            Some(2_147_483_000 + 100),
            "{kind:?}: the held row stays as it was"
        );
        assert_eq!(
            d.rows("select src_table, key from poison").await,
            ["(public.a,23)"]
        );
        let parked = d
            .rows("select src_table, key, origin_lsn is not null from poison_held")
            .await;
        assert_eq!(parked, ["(public.a,23,t)"], "{kind:?}");
        let charged = d
            .rows("select transform_table, column_name, key from column_failures")
            .await;
        assert_eq!(charged, [format!("({},total,23)", kind.name())], "{kind:?}");

        // The held row's own change is parked, not applied.
        step(&mut d, "update public.a set x = 23 where id = 23").await;
        assert_eq!(
            total(&d, kind, 23).await,
            Some(2_147_483_000 + 100),
            "{kind:?}"
        );
        release(&mut d, kind, "public.a", "23").await;
        assert_exact(&mut d, kind, "the release").await;
        assert_eq!(total(&d, kind, 23).await, Some(23 + 1000), "{kind:?}");
    }
}

/// A later `b` change on the key leaves a held row out of its fan-out, even
/// once its fields would compute: the row stays as it was until its
/// release re-derives it.
///
/// Pins the `join1_fan_out_held_row` plant.
#[tokio::test]
async fn a_fan_out_leaves_a_held_row_as_it_was() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind, A_ROWS, B_ROWS).await;
        seed_key_10(&mut d, &[23]).await;
        step(&mut d, "update public.b set w = 1000 where id = 1").await;
        step(
            &mut d,
            "update public.b set w = 0, tag = 'zero' where id = 1",
        )
        .await;
        assert_join_but(&d, kind, &[23], "a second fan-out").await;
        let held = d
            .rows(&format!(
                "select total, tag from {} where id = 23",
                kind.target()
            ))
            .await;
        assert_eq!(
            held,
            [format!("({},ten)", 2_147_483_000 + 100)],
            "{kind:?}: the held row isn't rewritten"
        );
        release(&mut d, kind, "public.a", "23").await;
        assert_exact(&mut d, kind, "the release").await;
    }
}

/// A page of the held row's own change that planned before the fan-out's page
/// held it (so it applies the change rather than park it) queues on the
/// key's `J` row behind that page, and applies after it commits: the row is
/// written over its hold, which stays, and the release re-derives it to the
/// same row. Nothing deadlocks.
#[tokio::test]
async fn a_change_of_the_row_planned_before_its_hold_applies_after_the_fan_out() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind, A_ROWS, B_ROWS).await;
        seed_key_10(&mut d, &[23]).await;
        write(&d, "update public.b set w = 1000 where id = 1").await;
        let fan_out = d.seal().await;
        write(&d, "update public.a set x = 23 where id = 23").await;
        let change = d.seal().await;
        let target = kind.target();
        let mut held = d
            .drain_frozen(fan_out, "fan_out", &[(PausePoint::BeforeCommit, &target)])
            .await;
        let reached = held.reached(PausePoint::BeforeCommit).await;
        let queued = d.drain_frozen(change, "change", &[]).await;
        d.wait_blocked_behind(reached.backend_pid).await;
        d.release(&mut held, PausePoint::BeforeCommit).await;
        held.finish().await;
        queued.finish().await;
        let deadlocks = d.deadlocks_logged();
        assert!(deadlocks.is_empty(), "{deadlocks:?}");
        assert_eq!(d.rows("select key from poison").await, ["(23)"], "{kind:?}");
        assert_eq!(total(&d, kind, 23).await, Some(23 + 1000), "{kind:?}");
        assert_join_but(&d, kind, &[], "the change over the hold").await;
        release(&mut d, kind, "public.a", "23").await;
        assert_exact(&mut d, kind, "the release").await;
    }
}

/// A release of a held row while a `b` change on its key waits in an earlier
/// batch: the change's fan-out runs before the release's `Recompute`
/// drains, so the release re-derives the row's `R` entry from its live row
/// in its own transaction, or the fan-out would read the state the hold
/// left and hold the row again.
///
/// Pins the `join1_release_no_rederive` plant.
#[tokio::test]
async fn a_release_before_a_pending_fan_out_of_its_key_stays_released() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind, A_ROWS, B_ROWS).await;
        seed_key_10(&mut d, &[23]).await;
        step(&mut d, "update public.b set w = 1000 where id = 1").await;
        step(&mut d, "update public.a set x = 23 where id = 23").await;
        write(&d, "update public.b set w = 1001 where id = 1").await;
        let pending = d.seal().await;
        let held = quarantine::release_key(d.pool(), kind.name(), "public.a", "23")
            .await
            .expect("release the key");
        assert_eq!(held, 1);
        d.drain(pending, "pending").await;
        assert!(
            d.rows("select key from poison").await.is_empty(),
            "{kind:?}: the pending fan-out held the released row again"
        );
        assert_exact(&mut d, kind, "the release").await;
    }
}

/// A release whose Re-derive needs the key's `J` row while a `b` page on the
/// key holds it queues behind the page, then re-derives the row from the
/// `M` that page committed. Nothing deadlocks.
#[tokio::test]
async fn a_release_queues_behind_a_page_on_its_key() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind, A_ROWS, B_ROWS).await;
        seed_key_10(&mut d, &[23]).await;
        step(&mut d, "update public.b set w = 1000 where id = 1").await;
        step(&mut d, "update public.a set x = 23 where id = 23").await;
        write(&d, "update public.b set w = 1001 where id = 1").await;
        let batch = d.seal().await;
        let target = kind.target();
        let mut page = d
            .drain_frozen(batch, "page", &[(PausePoint::BeforeCommit, &target)])
            .await;
        let reached = page.reached(PausePoint::BeforeCommit).await;
        let name = kind.name();
        let release = d
            .run_frozen(&[], move |pool| async move {
                quarantine::release_key(&pool, name, "public.a", "23").await
            })
            .await;
        d.wait_blocked_behind(reached.backend_pid).await;
        d.release(&mut page, PausePoint::BeforeCommit).await;
        page.finish().await;
        assert_eq!(release.finish().await, 1, "{kind:?}");
        let deadlocks = d.deadlocks_logged();
        assert!(deadlocks.is_empty(), "{deadlocks:?}");
        assert_eq!(total(&d, kind, 23).await, Some(23 + 1001), "{kind:?}");
        assert_exact(&mut d, kind, "the release").await;
    }
}

/// A fan-out whose failing rows its bisection can't all name within its
/// attempts halts the transform rather than commit with a row unwritten,
/// naming the rows it found; nothing is held.
#[tokio::test]
async fn a_fan_out_failing_on_more_rows_than_its_bisection_names_halts() {
    let _serial = SERIAL.lock().await;
    let kind = Kind::Left;
    let mut d = start(kind, A_ROWS, B_ROWS).await;
    let big: Vec<i32> = (5..=40).collect();
    seed_key_10(&mut d, &big).await;
    let rows: Vec<String> = (41..=400)
        .map(|id| format!("({id}, 10, 2147483000, 'r')"))
        .collect();
    step(
        &mut d,
        &format!("insert into public.a values {}", rows.join(", ")),
    )
    .await;
    step(&mut d, "update public.b set w = 1000 where id = 1").await;
    let (status, reason): (String, Option<String>) = {
        let row = d
            .ctl
            .query_one(
                "select d.status::text, \
                        (select f.error from capture_failures f where f.transform_id = d.id \
                         order by f.error limit 1) \
                 from transform_definitions d where d.target_table = $1",
                &[&kind.target()],
            )
            .await
            .expect("read the status");
        (row.get(0), row.get(1))
    };
    assert_eq!(status, "paused");
    let reason = reason.expect("a reason");
    assert!(
        reason.contains("fan-out fails on more rows")
            && reason.contains("(total: integer out of range)")
            && reason.contains("maybe more"),
        "{reason}"
    );
    assert!(d.rows("select key from poison").await.is_empty());
    assert_eq!(
        total(&d, kind, 5).await,
        Some(2_147_483_000 + 100),
        "nothing written"
    );
}

/// Each row a fan-out holds counts toward its column's fuse and the
/// definition's whole-transform fuse, both checked once the page commits:
/// four held rows trip neither, and a fifth trips both (each row is held on
/// its first failure, as a build chunk holds a key).
#[tokio::test]
async fn held_fan_out_rows_count_toward_the_fuses() {
    let _serial = SERIAL.lock().await;
    let kind = Kind::Left;
    let mut d = start(kind, A_ROWS, B_ROWS).await;
    seed_key_10(&mut d, &[11, 12, 13, 14, 15]).await;
    // Row 15 overflows only from 2000 on.
    step(&mut d, "update public.a set x = 2147482000 where id = 15").await;
    step(&mut d, "update public.b set w = 1000 where id = 1").await;
    assert_eq!(status(&d, &kind.target()).await, "live");
    let deaths = d
        .rows("select deaths from column_deaths where column_name = 'total'")
        .await;
    assert_eq!(deaths, ["(4)"]);
    assert!(d.rows("select 1 from column_status").await.is_empty());
    step(&mut d, "update public.b set w = 2000 where id = 1").await;
    assert_eq!(status(&d, &kind.target()).await, "quarantined");
    let paused = d
        .rows("select transform_table, column_name, local_fuse from column_status")
        .await;
    assert_eq!(paused, ["(jl,total,t)"]);
    assert_eq!(
        d.rows("select key from poison order by key").await,
        ["(11)", "(12)", "(13)", "(14)", "(15)"]
    );
}

/// A pause of a 1-1 `JOIN`'s column bumps both tables' version fences, as
/// the pages of either side write its fields: a `b` page that planned the
/// column as live and holds `b`'s fence makes the pause wait for its commit,
/// and the column is frozen from the pause's return.
#[tokio::test]
async fn a_column_pause_waits_for_a_page_of_the_joined_table() {
    let _serial = SERIAL.lock().await;
    let kind = Kind::Left;
    let mut d = start(kind, A_ROWS, B_ROWS).await;
    write(&d, "update public.b set w = 500 where id = 1").await;
    let batch = d.seal().await;
    let target = kind.target();
    let mut page = d
        .drain_frozen(batch, "page", &[(PausePoint::BeforeCommit, &target)])
        .await;
    let reached = page.reached(PausePoint::BeforeCommit).await;
    let pause = d
        .run_frozen(&[], |pool| async move {
            quarantine::pause_column(&pool, "jl", "total").await
        })
        .await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let queued = loop {
        let blocked: bool = d
            .ctl
            .query_one(
                "select exists (select 1 from pg_stat_activity \
                 where $1 = any(pg_blocking_pids(pid)))",
                &[&reached.backend_pid],
            )
            .await
            .expect("read pg_stat_activity")
            .get(0);
        if blocked || pause.is_finished() {
            break blocked;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the pause neither queued nor finished"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    d.release(&mut page, PausePoint::BeforeCommit).await;
    page.finish().await;
    pause.finish().await;
    assert!(queued, "the pause ran beside a page that writes the column");
    assert_eq!(total(&d, kind, 1).await, Some(1 + 500));
    step(&mut d, "update public.b set w = 600 where id = 1").await;
    assert_eq!(
        total(&d, kind, 1).await,
        Some(1 + 500),
        "the paused column is frozen"
    );
}

/// A key of `b` held in quarantine (its change fails alone: here a check on
/// the target refuses a row of its fan-out, which is no data exception, so
/// the drain's isolation charges the `b` change until it evicts it) leaves
/// `M`'s entry as it last applied, and the `a` rows on its key keep joining
/// that entry. Its release re-derives the `b` row and fans out to every row
/// on its keys.
#[tokio::test]
async fn a_held_key_of_b_is_released_by_a_rederive_and_its_fan_out() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind, A_ROWS, B_ROWS).await;
        d.ctl
            .batch_execute(&format!(
                "alter table {} add constraint small check (total < 5000)",
                kind.target()
            ))
            .await
            .expect("add the check");
        write(&d, "update public.b set w = 9999, tag = 'big' where id = 1").await;
        let batch = d.seal().await;
        let mut drained = false;
        for _ in 0..12 {
            if trellis::staging::apply::drain_once(d.pool(), batch, "page", 1, "j1_wake")
                .await
                .is_ok()
            {
                drained = true;
                break;
            }
        }
        assert!(drained, "{kind:?}: the page drains once the b key is held");
        assert_eq!(
            d.rows("select src_table, key from poison").await,
            ["(public.b,1)"]
        );
        assert_eq!(total(&d, kind, 1).await, Some(1 + 100), "{kind:?}");
        // An `a` row moving onto the held key joins `M`'s entry as it last
        // applied.
        step(&mut d, "update public.a set k = 10 where id = 2").await;
        assert_eq!(total(&d, kind, 2).await, Some(2 + 100), "{kind:?}");
        d.ctl
            .batch_execute(&format!(
                "alter table {} drop constraint small",
                kind.target()
            ))
            .await
            .expect("drop the check");
        release(&mut d, kind, "public.b", "1").await;
        assert_exact(&mut d, kind, "the release of b").await;
        assert_eq!(total(&d, kind, 2).await, Some(2 + 9999), "{kind:?}");
    }
}

/// An aggregate's target as `a`, keyed by its nullable `GROUP BY` columns:
/// a row with a `NULL` part that a fan-out fails on is held under its key
/// text, a later fan-out leaves it out, and its release re-derives it.
#[tokio::test]
async fn a_held_row_of_a_keyed_by_null_is_released() {
    let _serial = SERIAL.lock().await;
    for (word, sql) in [("LEFT", "left"), ("INNER", "inner")] {
        set_enabled(true);
        let mut d = Driver::start(
            "create table public.src (id integer primary key, g integer, h integer); \
             create table public.b (id integer primary key, k integer, w bigint); \
             create unique index on public.b (k); \
             insert into public.src values (1, 1, 1), (2, 1, null), (3, 1, null), \
                                           (4, 2, null); \
             insert into public.b values (1, 1, 100), (2, 2, 200)",
            &[("g", int4()), ("h", int4())],
            &[
                "TRANSFORM agg FROM public.src GROUP BY g, h WITHOUT SOURCE INDEX \
               SELECT count(*) AS n",
            ],
            &["public.src"],
        )
        .await;
        trellis::defs::install_definition(
            d.pool(),
            &format!(
                "TRANSFORM jn FROM public.agg {word} JOIN public.b ON agg.g = b.k \
                 SELECT agg.n + COALESCE(b.w, 0) AS total"
            ),
            &HashMap::from([("total".to_string(), int8())]),
            "public",
        )
        .await
        .expect("define the 1-1 JOIN");
        capture(&mut d, "public.b").await;
        trellis::intake::harness::settle_builds(d.pool()).await;
        d.settle().await;
        let expected = format!(
            "select agg.g, agg.h, agg.n + coalesce(b.w, 0) from public.agg agg \
             {sql} join public.b b on agg.g = b.k order by 1, 2"
        );
        let actual = "select g, h, total from public.jn order by 1, 2";
        assert_eq!(
            d.rows(actual).await,
            d.rows(&expected).await,
            "{word}: the build"
        );

        // `(1, NULL)` counts 2, and overflows; `(1, 1)` counts 1, and fits.
        step(
            &mut d,
            "update public.b set w = 9223372036854775806 where id = 1",
        )
        .await;
        drain_rounds(&mut d).await;
        assert_eq!(status(&d, "public.jn").await, "live", "{word}");
        let held: Vec<String> = d
            .ctl
            .query("select key from poison where src_table = 'public.agg'", &[])
            .await
            .expect("read poison")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(held.len(), 1, "{word}: {held:?}");
        let rows = d.rows(actual).await;
        assert!(
            rows.contains(&"(1,,102)".to_string())
                && rows.contains(&"(1,1,9223372036854775807)".to_string()),
            "{word}: {rows:?}"
        );
        step(&mut d, "update public.b set w = 1000 where id = 1").await;
        drain_rounds(&mut d).await;
        assert!(
            d.rows(actual).await.contains(&"(1,,102)".to_string()),
            "{word}: the held row is left out of the fan-out"
        );
        let released = quarantine::release_key(d.pool(), "jn", "public.agg", &held[0])
            .await
            .expect("release the key");
        assert_eq!(released, 1);
        d.settle().await;
        assert_eq!(
            d.rows(actual).await,
            d.rows(&expected).await,
            "{word}: the release"
        );
        assert!(d.rows("select key from poison").await.is_empty());
    }
}

/// A 1-1 target as `a`: the row a fan-out holds is held under the upstream's
/// target, and its release stages the Re-derive there.
#[tokio::test]
async fn a_held_row_of_a_chained_a_side_is_released() {
    let _serial = SERIAL.lock().await;
    set_enabled(true);
    let mut d = Driver::start(
        "create table public.asrc (id integer primary key, k integer, x integer); \
         create table public.b (id integer primary key, k integer, w integer); \
         create unique index on public.b (k); \
         insert into public.asrc values (1, 10, 1), (2, 10, 2147483000), (3, 20, 3); \
         insert into public.b values (1, 10, 100), (2, 20, 200)",
        &[("k", int4()), ("x", int4())],
        &["TRANSFORM ta FROM public.asrc SELECT k AS k, x AS x"],
        &["public.asrc"],
    )
    .await;
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM jc FROM public.ta LEFT JOIN public.b ON ta.k = b.k \
         SELECT ta.x + COALESCE(b.w, 0) AS total",
        &HashMap::from([("total".to_string(), int4())]),
        "public",
    )
    .await
    .expect("define the join over the 1-1 target");
    capture(&mut d, "public.b").await;
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    let expected = "select a.id, a.x + coalesce(b.w, 0) from public.asrc a \
                    left join public.b b on a.k = b.k order by a.id";
    let actual = "select id, total from public.jc order by id";
    step(&mut d, "update public.b set w = 1000 where id = 1").await;
    drain_rounds(&mut d).await;
    assert_eq!(
        d.rows("select src_table, key from poison").await,
        ["(public.ta,2)"]
    );
    assert_eq!(
        d.rows(actual).await,
        [
            "(1,1001)",
            &format!("(2,{})", 2_147_483_000 + 100),
            "(3,203)"
        ]
    );
    step(&mut d, "update public.asrc set x = 2 where id = 2").await;
    drain_rounds(&mut d).await;
    let released = quarantine::release_key(d.pool(), "jc", "public.ta", "2")
        .await
        .expect("release the key");
    assert_eq!(released, 1);
    d.settle().await;
    assert_eq!(d.rows(actual).await, d.rows(expected).await, "the release");
    let status = d
        .rows("select status::text from transform_definitions order by id")
        .await;
    assert_eq!(status, ["(live)", "(live)"]);
}

/// Seals and drains a few rounds, so what a page staged downstream (the
/// seam's rows) applies too: `Driver::settle` waits for nothing to be
/// pending, which a held key's parked work never is until its release.
async fn drain_rounds(d: &mut Driver) {
    for _ in 0..3 {
        let batch = d.seal().await;
        d.drain(batch, "round").await;
    }
}

/// Captures `table`.
async fn capture(d: &mut Driver, table: &str) {
    let catalog = trellis::capture::columns::load_catalog(&d.ctl)
        .await
        .expect("load the catalog");
    let spec = trellis::capture::columns::capture_spec(&d.ctl, &catalog, table)
        .await
        .expect("capture spec");
    match trellis::capture::install::reconcile(&mut d.ctl, DEFAULT_SCHEMA, &spec, None)
        .await
        .expect("capture the table")
    {
        trellis::capture::install::Progress::Done(_) => {}
        trellis::capture::install::Progress::Waiting(wait) => panic!("capture waited: {wait}"),
    }
}

/// A rebuild's start takes the definition's row `FOR UPDATE` and then the
/// pair's lock row. A `b` page whose fan-out holds a row writes `poison`,
/// whose foreign key takes the definition's row `FOR KEY SHARE`, so the page
/// takes that lock before its lock row: taken only by the hold, a start that
/// arrived while the page ran would hold the definition's row and queue on
/// the lock row while the page queued on the definition's row. The start is
/// played by hand, as in the interleavings' truncate test, and queues behind
/// the page.
#[tokio::test]
async fn a_fan_out_that_holds_a_row_and_a_rebuild_start_never_deadlock() {
    let _serial = SERIAL.lock().await;
    let kind = Kind::Left;
    let mut d = start(kind, A_ROWS, B_ROWS).await;
    seed_key_10(&mut d, &[23]).await;
    let id = definition_id(&d, &kind.target()).await;
    write(&d, "update public.b set w = 1000 where id = 1").await;
    let batch = d.seal().await;
    let target = kind.target();
    let mut page = d
        .drain_frozen(
            batch,
            "page",
            &[(PausePoint::BeforeJoin1Recompute, &target)],
        )
        .await;
    let reached = page.reached(PausePoint::BeforeJoin1Recompute).await;
    // The start: the definition's row, then the lock row.
    let start = d.user().await;
    let lock = format!("select 1 from {DEFAULT_SCHEMA}.join1_{id}_l for update");
    let start_lock = tokio::spawn(async move {
        start.batch_execute("begin").await?;
        start
            .execute(
                "select 1 from transform_definitions where id = $1 for update",
                &[&id],
            )
            .await?;
        start.execute(&lock, &[]).await?;
        start.batch_execute("commit").await
    });
    d.wait_blocked_behind(reached.backend_pid).await;
    d.release(&mut page, PausePoint::BeforeJoin1Recompute).await;
    let page_result = page.finish_result().await;
    let start_result = start_lock.await.expect("the start task");
    let deadlocks = d.deadlocks_logged();
    assert!(
        deadlocks.is_empty(),
        "page {:?}, start {start_result:?}: {deadlocks:?}",
        page_result.as_ref().map(|_| ())
    );
    page_result.expect("the page");
    start_result.expect("the start");
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
    assert_eq!(d.rows("select key from poison").await, ["(23)"]);
}

/// A held row across a `TRUNCATE b` and the rebuild it starts: the rebuild's
/// chunks and sweep leave the row as it was (an `INNER` row stays though its
/// `b` row is gone), every other row is rebuilt, and the release re-derives
/// the row from the rebuilt `M`.
#[tokio::test]
async fn a_held_row_stays_through_a_rebuild_and_its_release_re_derives_it() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind, A_ROWS, B_ROWS).await;
        seed_key_10(&mut d, &[23]).await;
        step(&mut d, "update public.b set w = 1000 where id = 1").await;
        let before = d
            .rows(&format!(
                "select total, tag from {} where id = 23",
                kind.target()
            ))
            .await;
        step(
            &mut d,
            "truncate public.b; insert into public.b values (1, 10, 0, 'zero')",
        )
        .await;
        trellis::staging::build::settle_builds_with(d.pool(), 3).await;
        assert_eq!(status(&d, &kind.target()).await, "live", "{kind:?}");
        assert_join_but(&d, kind, &[23], "the rebuild").await;
        assert_eq!(
            d.rows(&format!(
                "select total, tag from {} where id = 23",
                kind.target()
            ))
            .await,
            before,
            "{kind:?}: the held row stays as it was"
        );
        release(&mut d, kind, "public.a", "23").await;
        assert_exact(&mut d, kind, "the release").await;
        assert_eq!(total(&d, kind, 23).await, Some(2_147_483_000), "{kind:?}");
        let deadlocks = d.deadlocks_logged();
        assert!(deadlocks.is_empty(), "{deadlocks:?}");
    }
}

/// An `INNER` row whose `b` row is deleted while it is held stays, as every
/// held row does, and its release deletes it.
#[tokio::test]
async fn an_inner_held_row_whose_b_row_goes_is_deleted_by_its_release() {
    let _serial = SERIAL.lock().await;
    let kind = Kind::Inner;
    let mut d = start(kind, A_ROWS, B_ROWS).await;
    seed_key_10(&mut d, &[23]).await;
    step(&mut d, "update public.b set w = 1000 where id = 1").await;
    step(&mut d, "delete from public.b where id = 1").await;
    assert_join_but(&d, kind, &[23], "the delete").await;
    assert_eq!(total(&d, kind, 23).await, Some(2_147_483_000 + 100));
    release(&mut d, kind, "public.a", "23").await;
    assert_exact(&mut d, kind, "the release").await;
    assert_eq!(total(&d, kind, 23).await, None);
}

/// A resume of a 1-1 `JOIN`'s paused column rebuilds the target in place,
/// since the pages of either table left the column as it was while it was
/// paused: the rows a `b` change reached then, and an `a` change, get the
/// column back. A 1-1 `JOIN` has no field build of its own.
#[tokio::test]
async fn a_column_resume_rebuilds_the_rows_written_while_it_was_paused() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(kind, A_ROWS, B_ROWS).await;
        quarantine::pause_column(d.pool(), kind.name(), "total")
            .await
            .expect("pause the column");
        step(&mut d, "update public.b set w = 500 where id = 1").await;
        step(&mut d, "update public.a set x = 7 where id = 2").await;
        assert_eq!(total(&d, kind, 1).await, Some(1 + 100), "{kind:?}: paused");
        assert_eq!(total(&d, kind, 2).await, Some(2 + 200), "{kind:?}: paused");
        let resumed = quarantine::resume_column(d.pool(), kind.name(), "total")
            .await
            .expect("resume the column");
        assert_eq!(resumed, [(kind.name().to_string(), "total".to_string())]);
        assert_eq!(status(&d, &kind.target()).await, "backfilling", "{kind:?}");
        trellis::staging::build::settle_builds_with(d.pool(), 3).await;
        assert_exact(&mut d, kind, "the column's resume").await;
        let deadlocks = d.deadlocks_logged();
        assert!(deadlocks.is_empty(), "{deadlocks:?}");
    }
}
