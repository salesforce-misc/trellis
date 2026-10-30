//! Deterministic interleavings of the drain on 1-1 `JOIN` targets (ADR-0021,
//! epic #1392), ported from the #1371 and #1375 models: `a` changes, `J`,
//! `R`, the recompute and the target write, the minimal build, `b` changes
//! and their fan-out, and mixed pages.
//!
//! Every test turns the dev knob on before its driver starts. Each scenario
//! seeds `a` and `b`, defines the transform (the minimal build fills `M`,
//! `R`, `J` and the target), writes changes, seals them into batches and
//! drives the pages by hand (`support/drain_driver.rs`): one page frozen at a
//! [`PausePoint`] of its apply while another runs, which either queues behind
//! the frozen page (asserted with `pg_blocking_pids`) or runs through.
//! Nothing sleeps or polls for convergence. After each scenario the pipeline
//! settles and the target equals the live join
//! (`trellis::staging::one_to_one_join_diverging_keys`), `R` equals `a` and
//! `M` equals `b`.
//!
//! A scenario runs on both join kinds, `LEFT` and `INNER`. A test is named
//! for the #1371 or #1375 family it ports, where it has one:
//!
//! - `r17_…`: a stale Apply after a Re-derive (R17, the #392 shape).
//! - `n…`: `NULL` join keys (N1–N6).
//! - `f1_…` to `f15_…`: #1371's families of an `a` writer against a `b`
//!   writer, each run with either page frozen at its commit while the other
//!   queues on `J`: F1 an `a` insert ‖ a `b` insert of its key, F2 ‖ a `b`
//!   delete of it, F3 an `a` move `k1 → k2` ‖ a `b` update of `k2`, F4 ‖ of
//!   `k1` (and a `b` row moving off a key), F5 an `a` delete ‖ a `b` update
//!   of its key, F7/F7b a `b` unique-key hand-off applied out of order, F13 a
//!   fan-out past one batch, F15 a new `b` row's insert and update applied
//!   out of order.
//! - `m…`: #1375's mixed pages (M1–M6): a page holding an `a` key move and
//!   the `b` change of its new key, racing another mixed page.
//!
//! - `r…`: #1375's Re-derives of either side (R1–R19) against a change of
//!   the same row, of the other side, or a key move: R17 a stale Apply after
//!   a Re-derive, R18/R19 a row that moved onto a key after the page's
//!   unlocked read of it, which the page then holds no `J` row for.
//!
//! - `t…`: #1375's truncates (T1–T8) of either side or both, alone or with
//!   later inserts, and the rebuild a `TRUNCATE` of `b` starts (#1399).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::integer::IntWidth;
use trellis::staging::ApplyError;
use trellis::staging::interleave::PausePoint;

const DDL: &str = "create table public.a (id integer primary key, k integer, x integer, \
                       y text); \
                   create table public.b (id integer primary key, k integer, w integer, \
                       tag text); \
                   create unique index on public.b (k); \
                   create index on public.a (k)";

/// The retry counters and the forced key moves are process-wide
/// (`trellis::dev::interleave`), so these tests run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL.lock().await
}

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// One join kind: `LEFT` keeps an `a` row with no `b` match, `INNER` drops it.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Left,
    Inner,
}

const KINDS: [Kind; 2] = [Kind::Left, Kind::Inner];

impl Kind {
    fn target(self) -> &'static str {
        match self {
            Kind::Left => "public.jl",
            Kind::Inner => "public.ji",
        }
    }

    fn definition(self) -> String {
        let (name, word) = match self {
            Kind::Left => ("jl", "LEFT JOIN"),
            Kind::Inner => ("ji", "INNER JOIN"),
        };
        format!(
            "TRANSFORM {name} FROM public.a {word} public.b ON a.k = b.k \
             SELECT a.x + COALESCE(b.w, 0) AS total, b.tag AS tag, a.y AS y"
        )
    }

    /// The target's rows as the live join computes them.
    fn expected(self) -> &'static str {
        match self {
            Kind::Left => {
                "select a.id, a.x + coalesce(b.w, 0) as total, b.tag, a.y \
                 from public.a left join public.b on a.k = b.k order by a.id"
            }
            Kind::Inner => {
                "select a.id, a.x + coalesce(b.w, 0) as total, b.tag, a.y \
                 from public.a join public.b on a.k = b.k order by a.id"
            }
        }
    }

    fn actual(self) -> String {
        format!(
            "select id, total, tag, y from {} order by id",
            self.target()
        )
    }
}

/// A database with `a` seeded with `a_rows` `(id, k, x, y)` and `b` with
/// `b_rows` `(id, k, w, tag)`, the `kinds` defined and built, `a` captured.
async fn start_kinds(kinds: &[Kind], a_rows: &str, b_rows: &str) -> Driver {
    set_enabled(true);
    let ddl = format!(
        "{DDL}; {}{}",
        if a_rows.is_empty() {
            String::new()
        } else {
            format!("insert into public.a values {a_rows}; ")
        },
        if b_rows.is_empty() {
            String::new()
        } else {
            format!("insert into public.b values {b_rows}")
        }
    );
    let definitions: Vec<String> = kinds.iter().map(|k| k.definition()).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    Driver::start(
        &ddl,
        &[
            ("id", int4()),
            ("k", int4()),
            ("x", int4()),
            ("y", ValueType::Text),
        ],
        &definitions,
        &["public.a", "public.b"],
    )
    .await
}

/// Both kinds, for the scenarios that don't freeze a page.
async fn start(a_rows: &str, b_rows: &str) -> Driver {
    start_kinds(&KINDS, a_rows, b_rows).await
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

/// The label a page's `R` pause points carry for `kind`'s definition.
async fn r_label(d: &Driver, kind: Kind) -> String {
    let id = definition_id(d, kind.target()).await;
    format!("{}.join1_{id}_r", trellis::config::DEFAULT_SCHEMA)
}

/// Asserts `kind`'s target equals the live join, and its `R` holds exactly
/// `a`'s live rows with their keys.
async fn assert_kind(d: &Driver, kind: Kind) {
    let actual = d.rows(&kind.actual()).await;
    let expected = d.rows(kind.expected()).await;
    assert_eq!(
        actual, expected,
        "{kind:?} target (left) differs from the live join (right)"
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), kind.target())
        .await
        .expect("self_check");
    assert!(diverging.is_empty(), "{kind:?} diverges at {diverging:?}");
    let id = definition_id(d, kind.target()).await;
    let r = format!("{}.join1_{id}_r", trellis::config::DEFAULT_SCHEMA);
    let held = d
        .rows(&format!(
            "select __from_key, k, x, y from {r} where not __tombstone order by 1"
        ))
        .await;
    let live = d
        .rows("select id::text, k, x, y from public.a order by id::text")
        .await;
    assert_eq!(held, live, "{kind:?} R (left) differs from a (right)");
    let m = format!("{}.join1_{id}_m", trellis::config::DEFAULT_SCHEMA);
    let held = d
        .rows(&format!(
            "select __from_key, k, w, tag from {m} where not __tombstone order by 1"
        ))
        .await;
    let live = d
        .rows("select id::text, k, w, tag from public.b order by id::text")
        .await;
    assert_eq!(held, live, "{kind:?} M (left) differs from b (right)");
}

/// Settles the pipeline and asserts every defined kind's target.
async fn assert_oracle(d: &mut Driver, kinds: &[Kind]) {
    d.settle().await;
    for kind in kinds {
        assert_kind(d, *kind).await;
    }
}

const A_ROWS: &str =
    "(1, 10, 1, 'one'), (2, 20, 2, 'two'), (3, 30, 3, 'three'), (4, null, 4, 'four')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty'), (3, 40, 400, 'forty')";

#[tokio::test]
async fn the_build_fills_the_target_r_m_and_j_from_the_live_tables() {
    let _serial = serial().await;
    let mut d = start(A_ROWS, B_ROWS).await;
    assert_oracle(&mut d, &KINDS).await;
    let left = d.rows(&Kind::Left.actual()).await;
    assert_eq!(left.len(), 4, "LEFT keeps every a row: {left:?}");
    let inner = d.rows(&Kind::Inner.actual()).await;
    assert_eq!(inner.len(), 2, "INNER keeps the matched a rows: {inner:?}");
    let id = definition_id(&d, Kind::Left.target()).await;
    let schema = trellis::config::DEFAULT_SCHEMA;
    let m = d
        .rows(&format!(
            "select __from_key, k, w, tag from {schema}.join1_{id}_m order by 1"
        ))
        .await;
    assert_eq!(m.len(), 3, "{m:?}");
    // `J` holds the non-null keys of both sides.
    let j = d
        .rows(&format!("select k from {schema}.join1_{id}_j order by 1"))
        .await;
    assert_eq!(j, ["(10)", "(20)", "(30)", "(40)"]);
}

#[tokio::test]
async fn a_changes_insert_update_move_and_delete_rows() {
    let _serial = serial().await;
    let mut d = start(A_ROWS, B_ROWS).await;
    // A new row on a matched key, one on an unmatched key, one with a NULL key.
    step(
        &mut d,
        "insert into public.a values (5, 10, 5, 'five'), (6, 99, 6, 'six'), (7, null, 7, 'seven')",
    )
    .await;
    assert_oracle(&mut d, &KINDS).await;
    // A non-key column, a key move onto and off a match, a key to NULL.
    for sql in [
        "update public.a set x = x + 10 where id = 1",
        "update public.a set k = 20 where id = 3",
        "update public.a set k = 99 where id = 2",
        "update public.a set k = null where id = 1",
        "update public.a set k = 40 where id = 4",
        // Deletes, and a re-insert of a deleted id.
        "delete from public.a where id in (2, 5)",
        "insert into public.a values (2, 10, 22, 'again')",
    ] {
        step(&mut d, sql).await;
        assert_oracle(&mut d, &KINDS).await;
    }
}

#[tokio::test]
async fn a_page_with_several_changes_to_one_row_applies_the_newest() {
    let _serial = serial().await;
    let mut d = start(A_ROWS, B_ROWS).await;
    write(&d, "update public.a set k = 20, x = 50 where id = 1").await;
    write(&d, "update public.a set k = 10, x = 60 where id = 1").await;
    write(&d, "delete from public.a where id = 2").await;
    write(&d, "insert into public.a values (2, 40, 70, 'back')").await;
    let batch = d.seal().await;
    d.drain(batch, "w").await;
    assert_oracle(&mut d, &KINDS).await;
}

/// A connection that stands for the `b` side's page (its step is #1397):
/// holds `J`'s row for `key` `FOR UPDATE` and rewrites what a `b` change on
/// the key rewrites, uncommitted, until [`BWriter::commit`].
struct BWriter {
    client: tokio_postgres::Client,
    pid: i32,
}

impl BWriter {
    /// Locks `J[key]`, then sets `w` on `b`'s row of the key and on its `M`
    /// entry, and rewrites the target rows of the `a` rows on the key to
    /// `x + w` (the fan-out), for `kind`.
    async fn start(d: &Driver, kind: Kind, key: i32, w: i32) -> Self {
        let client = d.user().await;
        let pid: i32 = client
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        let id = definition_id(d, kind.target()).await;
        let schema = trellis::config::DEFAULT_SCHEMA;
        client.batch_execute("begin").await.expect("begin");
        client
            .batch_execute(&format!(
                "select 1 from {schema}.join1_{id}_j where k = {key} for update; \
                 update public.b set w = {w} where k = {key}; \
                 update {schema}.join1_{id}_m set w = {w} where k = {key}; \
                 update {} t set total = a.x + {w} from public.a a \
                     where a.id = t.id and a.k = {key}",
                kind.target()
            ))
            .await
            .expect("a b-side writer");
        BWriter { client, pid }
    }

    async fn commit(self) {
        self.client.batch_execute("commit").await.expect("commit");
    }
}

/// A page on a key a `b`-side writer holds queues on `J` and reads `M` only
/// once it has the lock (`rd_mearly`): the new row is `x + the writer's w`.
/// Planted `join1_no_j_lock` leaves the page running through, and the row it
/// wrote keeps the old `w`.
#[tokio::test]
async fn an_a_page_holds_its_j_row_until_it_commits() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        let b = BWriter::start(&d, kind, 10, 999).await;
        write(&d, "insert into public.a values (5, 10, 5, 'five')").await;
        let batch = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let drain = d.drain_frozen(batch, "page", &[]).await;
        let blocked = queued_or_finished(&d, b.pid, &drain).await;
        b.commit().await;
        let result = drain.finish_result().await;
        if !blocked {
            ran_through(&mut d, kind, result, moves).await;
        }
        result.expect("the page");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// A page whose row moves off a key locks the key it leaves
/// (`join1_old_key_unlocked`): `R`'s held key is in the set.
#[tokio::test]
async fn a_row_moving_off_a_key_locks_the_key_it_leaves() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        let b = BWriter::start(&d, kind, 10, 555).await;
        write(&d, "update public.a set k = 20 where id = 1").await;
        let batch = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let drain = d.drain_frozen(batch, "page", &[]).await;
        let blocked = queued_or_finished(&d, b.pid, &drain).await;
        b.commit().await;
        let result = drain.finish_result().await;
        if !blocked {
            ran_through(&mut d, kind, result, moves).await;
        }
        result.expect("the page");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// The key an `R` entry holds moved after the page read its key set: the page
/// rolls back and its retry locks the new key too (`join1_no_recheck`). The
/// `b`-side writer holds the key the row moved to, so the retry queues on it.
#[tokio::test]
async fn a_key_that_moved_under_the_page_rolls_it_back_and_the_retry_locks_both() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        // Row 2 is on key 20. Two changes: 20 -> 40, then 40 -> 10.
        write(&d, "update public.a set k = 40 where id = 2").await;
        let first = d.seal().await;
        write(&d, "update public.a set k = 10 where id = 2").await;
        let second = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        // The later change's page reads its key set while `R` still holds 20.
        let mut late = d
            .drain_frozen(
                second,
                "late",
                &[(PausePoint::BeforeJoin1Lock, kind.target())],
            )
            .await;
        late.reached(PausePoint::BeforeJoin1Lock).await;
        // The earlier change moves `R` to 40, and a `b` writer takes J[40].
        d.drain(first, "early").await;
        let b = BWriter::start(&d, kind, 40, 777).await;
        d.release(&mut late, PausePoint::BeforeJoin1Lock).await;
        d.wait_blocked_behind(b.pid).await;
        assert_eq!(
            trellis::dev::interleave::join_key_moves(),
            moves + 1,
            "the page rolled back once on the moved key"
        );
        b.commit().await;
        late.finish().await;
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// A key that moves on every attempt surfaces after the join-key budget
/// ([`trellis::dev::interleave::force_join_key_moves`]); fewer moves retry
/// to a commit.
#[tokio::test]
async fn a_hot_moving_key_exhausts_the_budget_and_surfaces() {
    let _serial = serial().await;
    let mut d = start_kinds(&[Kind::Left], A_ROWS, B_ROWS).await;
    write(&d, "update public.a set x = 77 where id = 1").await;
    let batch = d.seal().await;
    let moves = trellis::dev::interleave::join_key_moves();
    trellis::dev::interleave::force_join_key_moves(1_000);
    let result = d
        .drain_frozen(batch, "hot", &[])
        .await
        .finish_result()
        .await;
    trellis::dev::interleave::force_join_key_moves(0);
    assert!(
        matches!(result, Err(ref e) if e.to_string().contains("join key")),
        "{result:?}"
    );
    assert_eq!(
        trellis::dev::interleave::join_key_moves() - moves,
        21,
        "the budget is 20 retries after the first attempt"
    );
    // Three moves retry to a commit.
    let moves = trellis::dev::interleave::join_key_moves();
    trellis::dev::interleave::force_join_key_moves(3);
    d.drain(batch, "hot").await;
    assert_eq!(trellis::dev::interleave::join_key_moves() - moves, 3);
    assert_oracle(&mut d, &[Kind::Left]).await;
}

/// Two pages on a row with a NULL join key take no `J` row: the entry lock
/// orders them (`join1_no_entry_lock`, N2–N6).
#[tokio::test]
async fn n_two_pages_on_a_null_keyed_row_queue_on_its_entry() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        let label = r_label(&d, kind).await;
        write(&d, "update public.a set x = 41 where id = 4").await;
        let first = d.seal().await;
        write(&d, "update public.a set x = 42 where id = 4").await;
        let second = d.seal().await;
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
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// R17, the #392 shape: a Re-derive read the row after two changes, then the
/// older change's Apply arrives. The entry's basis saw its transaction, so
/// it is refused (`join1_lsn_only`); the target keeps the re-derived value
/// after that page and after the other's.
#[tokio::test]
async fn r17_a_stale_apply_after_a_rederive_is_refused() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        write(&d, "update public.a set x = 11 where id = 1").await;
        let older = d.seal().await;
        // The newer change and a Re-derive request fold to one Re-derive.
        write(&d, "update public.a set x = 12 where id = 1").await;
        d.stage_recomputes("public.a", &["1"]).await;
        let rederive = d.seal().await;
        // The Re-derive reads x = 12; the older Apply must not put 11 back.
        d.drain(rederive, "rederive").await;
        assert_eq!(total(&d, kind, 1).await, Some(12 + 100));
        d.drain(older, "older").await;
        assert_eq!(total(&d, kind, 1).await, Some(12 + 100), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// An Apply older than the entry's applied position is refused even when the
/// entry has no basis (`join1_vis_only`).
#[tokio::test]
async fn an_older_apply_after_a_newer_one_is_refused() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        write(&d, "update public.a set x = 11 where id = 1").await;
        let older = d.seal().await;
        write(&d, "update public.a set x = 12 where id = 1").await;
        let newer = d.seal().await;
        d.drain(newer, "newer").await;
        d.drain(older, "older").await;
        assert_eq!(total(&d, kind, 1).await, Some(12 + 100), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// `kind`'s target `total` for `id`.
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

/// Runs `sql` on `a` with its capture triggers disabled, in one transaction:
/// a change no entry will ever hear about.
async fn write_uncaptured(d: &Driver, sql: &str) {
    write_uncaptured_on(d, "public.a", sql).await;
}

/// [`write_uncaptured`] on `table`. (Capture triggers are `ENABLE ALWAYS`, so
/// `session_replication_role = replica` doesn't skip them.)
async fn write_uncaptured_on(d: &Driver, table: &str, sql: &str) {
    let toggle = |action: &str| {
        format!(
            "do $$ declare t text; begin \
                 for t in select tgname from pg_trigger \
                          where tgrelid = '{table}'::regclass and not tgisinternal loop \
                     execute format('alter table {table} {action} trigger %I', t); \
                 end loop; \
             end $$;"
        )
    };
    d.ctl
        .batch_execute(&format!(
            "begin; {} {sql}; {} commit;",
            toggle("disable"),
            toggle("enable always")
        ))
        .await
        .expect("write past capture");
}

/// A Re-derive of a row that has no `R` entry reads its live key before the
/// `J` lock, so it commits on its first attempt, and writes the row.
#[tokio::test]
async fn a_rederive_of_a_row_with_no_entry_commits_on_its_first_attempt() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        write_uncaptured(&d, "insert into public.a values (8, 10, 8, 'eight')").await;
        d.stage_recomputes("public.a", &["8"]).await;
        let batch = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        d.drain(batch, "rederive").await;
        assert_eq!(
            trellis::dev::interleave::join_key_moves(),
            moves,
            "no retry for a row the entry doesn't hold yet"
        );
        assert_eq!(total(&d, kind, 8).await, Some(8 + 100), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// A Re-derive repairs a row that changed without capture, and drops one
/// that was deleted without it.
#[tokio::test]
async fn a_rederive_repairs_uncaptured_changes() {
    let _serial = serial().await;
    let mut d = start(A_ROWS, B_ROWS).await;
    write_uncaptured(&d, "update public.a set x = 100, k = 20 where id = 1").await;
    write_uncaptured(&d, "delete from public.a where id = 2").await;
    d.stage_recomputes("public.a", &["1", "2"]).await;
    let batch = d.seal().await;
    d.drain(batch, "rederive").await;
    assert_oracle(&mut d, &KINDS).await;
}

/// A Re-derive whose row moved keys after the page's unlocked read finds a
/// key outside its locked set: it rolls back once and its retry commits.
#[tokio::test]
async fn a_rederive_whose_row_moved_keys_after_the_unlocked_read_retries() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        d.stage_recomputes("public.a", &["1"]).await;
        let batch = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let mut page = d
            .drain_frozen(
                batch,
                "rederive",
                &[(PausePoint::BeforeJoin1Lock, kind.target())],
            )
            .await;
        page.reached(PausePoint::BeforeJoin1Lock).await;
        // Captured, into a later batch: the frozen page's read of `a` would
        // block a trigger toggle.
        write(&d, "update public.a set k = 20 where id = 1").await;
        d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
        page.finish().await;
        assert_eq!(
            trellis::dev::interleave::join_key_moves(),
            moves + 1,
            "the Re-derive's read found a key it hadn't locked"
        );
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// A page whose segment predates the build re-derives its keys rather than
/// applying their changes (the build-start rule): the Apply of a change that
/// the build's read already saw must not put an older image over it.
#[tokio::test]
async fn a_page_that_predates_the_build_rederives() {
    let _serial = serial().await;
    set_enabled(true);
    let ddl = format!(
        "{DDL}; insert into public.a values {A_ROWS}; insert into public.b values {B_ROWS}"
    );
    let mut d = Driver::start_unbuilt(
        &[],
        &ddl,
        &[
            ("id", int4()),
            ("k", int4()),
            ("x", int4()),
            ("y", ValueType::Text),
        ],
        &[&Kind::Left.definition()],
        &["public.a"],
    )
    .await;
    // Two changes to row 1 land in the ring after the build started.
    write(&d, "update public.a set x = 11 where id = 1").await;
    write(&d, "update public.a set x = 12 where id = 1").await;
    trellis::staging::build::settle_builds(d.pool()).await;
    d.settle().await;
    assert_kind(&d, Kind::Left).await;
}

/// A change that commits after a Re-derive's read statement is not in the
/// row the Re-derive wrote, so it must not be in the entry's basis either
/// (`join1_snapshot_after_read`, `rd_latesnap`): its Apply, drained after the
/// Re-derive, applies.
#[tokio::test]
async fn a_change_between_a_rederives_read_and_its_snapshot_is_applied() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        d.stage_recomputes("public.a", &["1"]).await;
        let rederive = d.seal().await;
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive",
                &[(PausePoint::AfterRederiveRead, kind.target())],
            )
            .await;
        page.reached(PausePoint::AfterRederiveRead).await;
        write(&d, "update public.a set x = 500 where id = 1").await;
        let change = d.seal().await;
        d.release(&mut page, PausePoint::AfterRederiveRead).await;
        page.finish().await;
        d.drain(change, "change").await;
        assert_eq!(total(&d, kind, 1).await, Some(500 + 100), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// Two pages on a row no `R` entry exists for yet: the newer page's
/// placeholder insert is uncommitted, so the older page queues on it rather
/// than overwriting it (`join1_no_placeholder`).
#[tokio::test]
async fn two_pages_on_a_new_row_queue_on_the_placeholder() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        let label = r_label(&d, kind).await;
        write(&d, "insert into public.a values (9, 10, 9, 'nine')").await;
        let older = d.seal().await;
        write(&d, "update public.a set x = 99 where id = 9").await;
        let newer = d.seal().await;
        let mut held = d
            .drain_frozen(newer, "newer", &[(PausePoint::AfterPlaceholders, &label)])
            .await;
        let reached = held.reached(PausePoint::AfterPlaceholders).await;
        let queued = d.drain_frozen(older, "older", &[]).await;
        d.wait_blocked_behind(reached.backend_pid).await;
        d.release(&mut held, PausePoint::AfterPlaceholders).await;
        held.finish().await;
        queued.finish().await;
        assert_eq!(total(&d, kind, 9).await, Some(99 + 100), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// A field that overflows for one row fails the page's statement with a data
/// exception; the drain isolates the row (it is held, like any 1-1 key whose
/// value a field cannot compute) and the page's other rows apply.
#[tokio::test]
async fn a_row_whose_field_overflows_is_isolated_and_the_rest_apply() {
    let _serial = serial().await;
    let mut d = start_kinds(&[Kind::Left], A_ROWS, B_ROWS).await;
    write(
        &d,
        "insert into public.a values (5, 10, 2147483647, 'big'), (6, 20, 6, 'six')",
    )
    .await;
    let batch = d.seal().await;
    // The orchestrating drain, which isolates a failing record.
    // A failing record is charged a death per drain until the threshold evicts it.
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
    assert!(drained, "the page drains once the row is evicted");
    assert_eq!(total(&d, Kind::Left, 6).await, Some(6 + 200));
    assert_eq!(
        total(&d, Kind::Left, 5).await,
        None,
        "the overflowing row is held"
    );
    let held = d
        .rows("select src_table, key from poison order by key")
        .await;
    assert_eq!(held, ["(public.a,5)"]);
}

/// Two live `M` entries on one key (a hand-off between `b` rows applied out of
/// order, #1397's): the recompute reads the one an Apply wrote over one only a
/// build or Re-derive wrote (no applied position), as the #1375 model orders
/// them (`applied desc nulls last`). The second entry is written by hand, as
/// a `b` page would leave it.
#[tokio::test]
async fn the_newest_m_entry_on_a_key_is_the_applied_one() {
    let _serial = serial().await;
    let mut d = start_kinds(&[Kind::Left], A_ROWS, B_ROWS).await;
    let id = definition_id(&d, Kind::Left.target()).await;
    let schema = trellis::config::DEFAULT_SCHEMA;
    // `b` row 1 holds key 10 from the build, with no applied position.
    d.ctl
        .batch_execute(&format!(
            "insert into {schema}.join1_{id}_m (__from_key, k, w, tag, __applied_lsn) \
             values ('9', 10, 5000, 'handed', '0/1')"
        ))
        .await
        .expect("a second live M entry on key 10");
    step(&mut d, "update public.a set x = 2 where id = 1").await;
    assert_eq!(total(&d, Kind::Left, 1).await, Some(2 + 5000));
}

/// The label a page's `M` pause points carry for `kind`'s definition.
async fn m_label(d: &Driver, kind: Kind) -> String {
    let id = definition_id(d, kind.target()).await;
    format!("{}.join1_{id}_m", trellis::config::DEFAULT_SCHEMA)
}

/// Waits until some backend queues on a lock `pid` holds (`true`), or until
/// `page` finishes without one having queued (`false`): a page that should
/// queue but runs through then fails on what it wrote, or on its error,
/// rather than on a timeout. Polls as `Driver::wait_blocked_behind` does; a
/// page that queues can't finish first, since `pid` holds its lock.
async fn queued_or_finished<T>(d: &Driver, pid: i32, page: &drain_driver::Running<T>) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
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
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// The end of a race whose second page didn't queue: it fails with what the
/// page did, the target it left or the error it surfaced.
async fn ran_through<T>(
    d: &mut Driver,
    kind: Kind,
    result: Result<T, ApplyError>,
    moves: u64,
) -> ! {
    let moved = trellis::dev::interleave::join_key_moves() - moves;
    if let Err(err) = result {
        panic!(
            "{kind:?}: the page ran without queueing on the other's lock and failed \
             ({moved} key-moved rollbacks): {err}"
        );
    }
    assert_oracle(d, &[kind]).await;
    panic!(
        "{kind:?}: the page ran without queueing on the other's lock ({moved} key-moved rollbacks)"
    );
}

/// Drains `first` frozen at its commit, then `second`, which must queue on a
/// lock the first holds (a `J` row). Lets both finish, asserts nothing
/// deadlocked, and checks the target.
async fn race(d: &mut Driver, kind: Kind, first: i64, second: i64) {
    let mut held = d
        .drain_frozen(first, "first", &[(PausePoint::BeforeCommit, kind.target())])
        .await;
    let reached = held.reached(PausePoint::BeforeCommit).await;
    let moves = trellis::dev::interleave::join_key_moves();
    let queued = d.drain_frozen(second, "second", &[]).await;
    let blocked = queued_or_finished(d, reached.backend_pid, &queued).await;
    d.release(&mut held, PausePoint::BeforeCommit).await;
    held.finish().await;
    let result = queued.finish_result().await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
    if !blocked {
        ran_through(d, kind, result, moves).await;
    }
    result.expect("the second page");
    assert_oracle(d, &[kind]).await;
}

/// Which of a family's two writers drains first, frozen at its commit.
#[derive(Debug, Clone, Copy)]
enum First {
    A,
    B,
}

/// One #1371 family: an `a` writer (`a_sql`) against a `b` writer (`b_sql`)
/// on a key they share, each in its own batch. For both join kinds, the `a`
/// page frozen at its commit while the `b` page queues, then the other way
/// round.
async fn family(a_sql: &str, b_sql: &str) {
    family_on(A_ROWS, B_ROWS, a_sql, b_sql).await;
}

async fn family_on(a_rows: &str, b_rows: &str, a_sql: &str, b_sql: &str) {
    for kind in KINDS {
        for first in [First::A, First::B] {
            let mut d = start_kinds(&[kind], a_rows, b_rows).await;
            write(&d, a_sql).await;
            let a = d.seal().await;
            write(&d, b_sql).await;
            let b = d.seal().await;
            match first {
                First::A => race(&mut d, kind, a, b).await,
                First::B => race(&mut d, kind, b, a).await,
            }
        }
    }
}

/// `b` changes apply through `M` and the fan-out: an insert matching an `a`
/// row, an update of a key with several `a` rows, a key move off and onto a
/// key, a key to `NULL`, a delete and a re-insert.
#[tokio::test]
async fn b_changes_insert_update_move_and_delete_rows() {
    let _serial = serial().await;
    let mut d = start(A_ROWS, B_ROWS).await;
    step(&mut d, "insert into public.a values (5, 10, 5, 'five')").await;
    for sql in [
        // Row 3's key 30 had no match.
        "insert into public.b values (4, 30, 300, 'thirty')",
        // Rows 1 and 5 are on key 10.
        "update public.b set w = 111, tag = 'TEN' where id = 1",
        // Off key 20 (row 2 loses its match), then onto it.
        "update public.b set k = 99 where id = 2",
        "update public.b set k = 20 where id = 3",
        "update public.b set k = null where id = 4",
        "delete from public.b where id = 1",
        "insert into public.b values (1, 10, 1000, 'back')",
    ] {
        step(&mut d, sql).await;
        assert_oracle(&mut d, &KINDS).await;
    }
}

/// A page with several changes to one `b` row applies the newest, and its
/// fan-out covers the key the row left as well as the one it ends on.
#[tokio::test]
async fn a_page_with_several_changes_to_one_b_row_applies_the_newest() {
    let _serial = serial().await;
    let mut d = start(A_ROWS, B_ROWS).await;
    write(&d, "update public.b set k = 30, w = 150 where id = 1").await;
    write(&d, "update public.b set k = 50, w = 160 where id = 1").await;
    write(&d, "delete from public.b where id = 2").await;
    write(&d, "insert into public.b values (2, 10, 170, 'moved')").await;
    let batch = d.seal().await;
    d.drain(batch, "w").await;
    assert_oracle(&mut d, &KINDS).await;
}

/// F1: an `a` insert on a key no `b` row has ‖ the `b` insert of that key.
/// The page that runs second queues on `J[30]` and reads the other side's
/// copy only once it has the lock, so whichever commits first, the other
/// sees it (`join1_m_before_j`, `join1_no_j_lock`).
#[tokio::test]
async fn f1_an_a_page_on_a_key_reads_m_only_after_the_b_page_on_it_commits() {
    let _serial = serial().await;
    family(
        "insert into public.a values (5, 30, 5, 'five')",
        "insert into public.b values (4, 30, 300, 'thirty')",
    )
    .await;
}

/// F2: an `a` insert ‖ the `b` delete of its key.
#[tokio::test]
async fn f2_an_a_insert_races_the_b_delete_of_its_key() {
    let _serial = serial().await;
    family(
        "insert into public.a values (5, 10, 5, 'five')",
        "delete from public.b where id = 1",
    )
    .await;
}

/// F3: an `a` key move `10 → 20` ‖ a `b` update of `20`.
#[tokio::test]
async fn f3_an_a_move_races_a_b_update_of_its_new_key() {
    let _serial = serial().await;
    family(
        "update public.a set k = 20 where id = 1",
        "update public.b set w = 222 where id = 2",
    )
    .await;
}

/// F4: an `a` key move `10 → 20` ‖ a `b` update of `10`, the key it leaves.
#[tokio::test]
async fn f4_an_a_move_races_a_b_update_of_the_key_it_leaves() {
    let _serial = serial().await;
    family(
        "update public.a set k = 20 where id = 1",
        "update public.b set w = 111 where id = 1",
    )
    .await;
}

/// F4 on the `b` side: a `b` row moving off key 10 locks the key it leaves,
/// which its `M` entry holds and its image's old key names
/// (`join1_old_key_unlocked`), so it queues behind an `a` page inserting a
/// row there, and its fan-out then finds that row.
#[tokio::test]
async fn f4_a_b_row_moving_off_a_key_locks_the_key_it_leaves() {
    let _serial = serial().await;
    family(
        "insert into public.a values (5, 10, 5, 'five')",
        "update public.b set k = 50 where id = 1",
    )
    .await;
}

/// F5: an `a` delete ‖ a `b` update of its key.
#[tokio::test]
async fn f5_an_a_delete_races_a_b_update_of_its_key() {
    let _serial = serial().await;
    family(
        "delete from public.a where id = 1",
        "update public.b set w = 111 where id = 1",
    )
    .await;
}

/// F7: a `b` unique-key hand-off (row 1 leaves key 10, row 2 takes it),
/// drained in the wrong order, with an `a` insert on the key in the take's
/// page. While only the take has applied, `M` holds two live entries on key
/// 10 and the recompute reads the one the newer Apply wrote; the leave then
/// recomputes the key again.
#[tokio::test]
async fn f7_a_b_hand_off_applied_out_of_order() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        write(&d, "update public.b set k = 60 where id = 1").await;
        let leave = d.seal().await;
        write(&d, "update public.b set k = 10 where id = 2").await;
        write(&d, "insert into public.a values (5, 10, 5, 'five')").await;
        let take = d.seal().await;
        d.drain(take, "take").await;
        assert_eq!(total(&d, kind, 1).await, Some(1 + 200), "{kind:?}");
        assert_eq!(total(&d, kind, 5).await, Some(5 + 200), "{kind:?}");
        d.drain(leave, "leave").await;
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// F7b: the two pages of a hand-off racing each other. Both lock key 10, so
/// the second queues on it.
#[tokio::test]
async fn f7b_two_hand_off_pages_race() {
    let _serial = serial().await;
    for kind in KINDS {
        for take_first in [true, false] {
            let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
            write(&d, "update public.b set k = 60 where id = 1").await;
            let leave = d.seal().await;
            write(&d, "update public.b set k = 10 where id = 2").await;
            let take = d.seal().await;
            if take_first {
                race(&mut d, kind, take, leave).await;
            } else {
                race(&mut d, kind, leave, take).await;
            }
        }
    }
}

/// F13: a fan-out past one keyset batch (25,000 `a` rows on key 10, three
/// batches of 10,000) ‖ an `a` page moving a row onto the key. Either page
/// frozen at its commit holds `J[10]` and the other queues.
#[tokio::test]
async fn f13_a_fan_out_of_several_batches_races_an_a_move_onto_its_key() {
    let _serial = serial().await;
    let a_rows = format!(
        "{A_ROWS}, {}",
        (100..25_100)
            .map(|id| format!("({id}, 10, {id}, 'many')"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    family_on(
        &a_rows,
        B_ROWS,
        "update public.a set k = 10 where id = 2",
        "update public.b set w = 999 where id = 1",
    )
    .await;
}

/// F15: a new `b` row's insert and update drained out of order, the update's
/// page holding its placeholder: the insert's page queues on it rather than
/// overwriting it (`join1_no_placeholder`), and is then refused by I2.
#[tokio::test]
async fn f15_a_new_b_rows_insert_and_update_applied_out_of_order() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        let label = m_label(&d, kind).await;
        write(&d, "insert into public.b values (4, 30, 300, 'thirty')").await;
        let older = d.seal().await;
        write(&d, "update public.b set w = 333 where id = 4").await;
        let newer = d.seal().await;
        let mut held = d
            .drain_frozen(newer, "newer", &[(PausePoint::AfterPlaceholders, &label)])
            .await;
        let reached = held.reached(PausePoint::AfterPlaceholders).await;
        let queued = d.drain_frozen(older, "older", &[]).await;
        d.wait_blocked_behind(reached.backend_pid).await;
        d.release(&mut held, PausePoint::AfterPlaceholders).await;
        held.finish().await;
        queued.finish().await;
        assert_eq!(total(&d, kind, 3).await, Some(3 + 333), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// An older `b` Apply drained after a newer one is refused by its `M` entry's
/// applied position (F12 on the `b` side, `join1_vis_only`).
#[tokio::test]
async fn an_older_b_apply_after_a_newer_one_is_refused() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        write(&d, "update public.b set w = 101 where id = 1").await;
        let older = d.seal().await;
        write(&d, "update public.b set w = 102 where id = 1").await;
        let newer = d.seal().await;
        d.drain(newer, "newer").await;
        d.drain(older, "older").await;
        assert_eq!(total(&d, kind, 1).await, Some(1 + 102), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// A mixed page: an `a` row moving onto key 20 and the `b` change of key 20,
/// in one page. The `a` row is written from the page's own new `M` entry, and
/// the fan-out recomputes the key's other rows. Then the two changes in two
/// pages, the `b` change drained before the `a` change and after it.
#[tokio::test]
async fn m_a_mixed_page_applies_an_a_move_and_the_b_change_of_its_new_key() {
    let _serial = serial().await;
    let a_change = "update public.a set k = 20 where id = 1";
    let b_change = "update public.b set w = 250 where id = 2";
    let mut d = start(A_ROWS, B_ROWS).await;
    write(&d, a_change).await;
    write(&d, b_change).await;
    let batch = d.seal().await;
    d.drain(batch, "mixed").await;
    assert_oracle(&mut d, &KINDS).await;
    for b_first in [true, false] {
        let mut d = start(A_ROWS, B_ROWS).await;
        write(&d, a_change).await;
        let a = d.seal().await;
        write(&d, b_change).await;
        let b = d.seal().await;
        let order = if b_first { [b, a] } else { [a, b] };
        for batch in order {
            d.drain(batch, "one").await;
        }
        assert_oracle(&mut d, &KINDS).await;
    }
}

/// Two mixed pages racing (M1–M6). In the first pair one moves `a` row 1
/// from 10 to 20 and changes `b`'s key 20, the other moves `a` row 2 from 20
/// to 10 and changes key 10. In the second the other page moves row 3 from 30
/// onto 10 instead, so its own `a` write touches no row the first page wrote,
/// and only `J[10]` stops its fan-out from reading row 1 on key 10 while the
/// first page moves it off (`join1_no_j_lock`). Each page locks both its keys
/// in one sorted `J` statement, so the second queues on the first and
/// neither deadlocks, in either order.
#[tokio::test]
async fn m_two_crossing_mixed_pages_queue_on_j_without_deadlock() {
    let _serial = serial().await;
    for other_a in [
        "update public.a set k = 10 where id = 2",
        "update public.a set k = 10 where id = 3",
    ] {
        for kind in KINDS {
            for one_first in [true, false] {
                let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
                write(&d, "update public.a set k = 20 where id = 1").await;
                write(&d, "update public.b set w = 250 where id = 2").await;
                let one = d.seal().await;
                write(&d, other_a).await;
                write(&d, "update public.b set w = 150 where id = 1").await;
                let two = d.seal().await;
                if one_first {
                    race(&mut d, kind, one, two).await;
                } else {
                    race(&mut d, kind, two, one).await;
                }
            }
        }
    }
}

/// A mixed page racing a `b` page on the key its `a` row moves to and a
/// page of `a` changes on the key it leaves.
#[tokio::test]
async fn m_a_mixed_page_races_pages_of_either_side() {
    let _serial = serial().await;
    for kind in KINDS {
        for (other, a_first) in [
            ("update public.b set w = 222 where id = 2", true),
            ("update public.b set w = 222 where id = 2", false),
            ("insert into public.a values (5, 10, 5, 'five')", true),
            ("insert into public.a values (5, 10, 5, 'five')", false),
        ] {
            let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
            write(&d, "update public.a set k = 20 where id = 1").await;
            write(&d, "update public.b set w = 111 where id = 1").await;
            let mixed = d.seal().await;
            write(&d, other).await;
            let single = d.seal().await;
            if a_first {
                race(&mut d, kind, mixed, single).await;
            } else {
                race(&mut d, kind, single, mixed).await;
            }
        }
    }
}

/// The status and pause reason of `kind`'s definition.
async fn paused_reason(d: &Driver, kind: Kind) -> (String, Option<String>) {
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
        .expect("read the definition's status");
    (row.get(0), row.get(1))
}

/// A page whose `b` changes would recompute more `a` rows than the fan-out
/// cap halts the transform with the reason, writing nothing.
#[tokio::test]
async fn a_fan_out_past_the_cap_halts_the_transform() {
    let _serial = serial().await;
    let mut d = start_kinds(&[Kind::Left], A_ROWS, B_ROWS).await;
    // Two `a` rows on key 10.
    step(&mut d, "insert into public.a values (5, 10, 5, 'five')").await;
    trellis::defs::join_one_to_one::set_max_fanout(1);
    write(&d, "update public.b set w = 999 where id = 1").await;
    let batch = d.seal().await;
    let result = d
        .drain_frozen(batch, "capped", &[])
        .await
        .finish_result()
        .await;
    trellis::defs::join_one_to_one::set_max_fanout(0);
    result.expect("the halt pauses the transform and the drain goes on");
    let (status, reason) = paused_reason(&d, Kind::Left).await;
    assert_eq!(status, "paused");
    let reason = reason.expect("a reason");
    assert!(
        reason.contains("more than 1 rows")
            && reason.contains("TRELLIS_ONE_TO_ONE_JOIN_MAX_FANOUT"),
        "{reason}"
    );
    assert_eq!(
        total(&d, Kind::Left, 1).await,
        Some(1 + 100),
        "nothing written"
    );
}

/// A field that fails on one row of a fan-out halts the transform, naming the
/// row and the field, rather than charging the `b` change: the fan-out's
/// batches are bisected under savepoints down to the row, whose fields are
/// probed one by one. Nothing is quarantined, and no other row is written.
#[tokio::test]
async fn a_field_failing_on_a_fan_out_row_halts_naming_the_row() {
    let _serial = serial().await;
    let mut d = start_kinds(&[Kind::Left], A_ROWS, B_ROWS).await;
    // Rows 5 to 40 on key 10; row 23 overflows `x + w` once `w` grows.
    let rows: Vec<String> = (5..=40)
        .map(|id| {
            let x = if id == 23 { 2_147_483_000 } else { id };
            format!("({id}, 10, {x}, 'r')")
        })
        .collect();
    step(
        &mut d,
        &format!("insert into public.a values {}", rows.join(", ")),
    )
    .await;
    write(&d, "update public.b set w = 1000 where id = 1").await;
    let batch = d.seal().await;
    d.drain(batch, "poisoned").await;
    let (status, reason) = paused_reason(&d, Kind::Left).await;
    assert_eq!(status, "paused");
    let reason = reason.expect("a reason");
    assert!(
        reason.contains("fan-out fails")
            && reason.contains("23 (total: integer out of range)")
            && !reason.contains("maybe more"),
        "{reason}"
    );
    assert_eq!(
        total(&d, Kind::Left, 5).await,
        Some(5 + 100),
        "nothing written"
    );
    let poisoned = d.rows("select key from poison").await;
    assert!(poisoned.is_empty(), "{poisoned:?}");
}

/// A Re-derive of `b` rows repairs changes capture never saw: a `w` update
/// on a key with an `a` row (its fan-out), a move onto a key, a delete, an
/// insert onto a `NULL` key and one onto a key no `a` row has.
#[tokio::test]
async fn a_b_rederive_repairs_uncaptured_changes() {
    let _serial = serial().await;
    let mut d = start(A_ROWS, B_ROWS).await;
    for sql in [
        "update public.b set w = 150 where id = 1",
        "update public.b set k = 30, tag = 'thirty' where id = 3",
        "delete from public.b where id = 2",
        "insert into public.b values (4, null, 4000, 'none')",
        "insert into public.b values (5, 50, 500, 'fifty')",
    ] {
        write_uncaptured_on(&d, "public.b", sql).await;
    }
    d.stage_recomputes("public.b", &["1", "2", "3", "4", "5"])
        .await;
    let batch = d.seal().await;
    d.drain(batch, "rederive b").await;
    assert_oracle(&mut d, &KINDS).await;
}

/// A Re-derive of a `b` row with no `M` entry reads its live key before the
/// `J` lock, so it commits on its first attempt, and its fan-out reaches the
/// `a` row on the key.
#[tokio::test]
async fn a_b_rederive_of_a_row_with_no_entry_commits_on_its_first_attempt() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        write_uncaptured_on(
            &d,
            "public.b",
            "insert into public.b values (4, 30, 300, 'thirty')",
        )
        .await;
        d.stage_recomputes("public.b", &["4"]).await;
        let batch = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        d.drain(batch, "rederive b").await;
        assert_eq!(
            trellis::dev::interleave::join_key_moves(),
            moves,
            "no retry for a row the entry doesn't hold yet"
        );
        assert_eq!(total(&d, kind, 3).await, Some(3 + 300), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// A page of `b` changes whose segment predates the build re-derives its
/// `b` rows (the build-start rule), as an `a` page does: an update, a key
/// move and a delete.
#[tokio::test]
async fn a_b_page_that_predates_the_build_rederives() {
    let _serial = serial().await;
    set_enabled(true);
    let ddl = format!(
        "{DDL}; insert into public.a values {A_ROWS}; insert into public.b values {B_ROWS}"
    );
    let mut d = Driver::start_unbuilt(
        &[],
        &ddl,
        &[
            ("id", int4()),
            ("k", int4()),
            ("x", int4()),
            ("y", ValueType::Text),
        ],
        &[&Kind::Left.definition()],
        &["public.a", "public.b"],
    )
    .await;
    write(&d, "update public.b set w = 111 where id = 1").await;
    write(&d, "update public.b set k = 30 where id = 3").await;
    write(&d, "delete from public.b where id = 2").await;
    trellis::staging::build::settle_builds(d.pool()).await;
    d.settle().await;
    assert_kind(&d, Kind::Left).await;
    let (status, reason) = paused_reason(&d, Kind::Left).await;
    assert_eq!(status, "live", "{reason:?}");
}

/// R18 (`brd_nokey`): a `b` row moves onto key 30 after a Re-derive page's
/// unlocked read of it, while an `a` page inserting a row on 30 holds `J[30]`
/// uncommitted. The Re-derive's read finds 30 outside its locked set, so it
/// rolls back and its retry queues on `J[30]`; its fan-out then reaches the
/// new `a` row. Planted `join1_brd_no_key` writes the `M` entry and fans out
/// on 30 without the lock, around the `a` row, which keeps no match.
#[tokio::test]
async fn r18_a_b_rederive_whose_row_moved_onto_a_held_key_retries() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        d.stage_recomputes("public.b", &["1"]).await;
        let rederive = d.seal().await;
        write(&d, "insert into public.a values (5, 30, 5, 'five')").await;
        let insert = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive b",
                &[(PausePoint::BeforeJoin1Lock, kind.target())],
            )
            .await;
        page.reached(PausePoint::BeforeJoin1Lock).await;
        // Captured, left for the settle to drain: the Re-derive's basis sees
        // it, so its Apply is refused.
        write(&d, "update public.b set k = 30 where id = 1").await;
        let mut held = d
            .drain_frozen(insert, "a", &[(PausePoint::BeforeCommit, kind.target())])
            .await;
        let reached = held.reached(PausePoint::BeforeCommit).await;
        d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
        let blocked = queued_or_finished(&d, reached.backend_pid, &page).await;
        d.release(&mut held, PausePoint::BeforeCommit).await;
        held.finish().await;
        let result = page.finish_result().await;
        if !blocked {
            ran_through(&mut d, kind, result, moves).await;
        }
        result.expect("the Re-derive page");
        assert_eq!(
            trellis::dev::interleave::join_key_moves(),
            moves + 1,
            "the Re-derive's read found a key it hadn't locked"
        );
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// R19 (`rd_nokey`), R18's mirror: an `a` row moves onto key 30 after a
/// Re-derive page's unlocked read of it, while a `b` page inserting a row on
/// 30 holds `J[30]` uncommitted. The retry queues on `J[30]` and reads the
/// new `M` entry. Planted `join1_rd_no_key` writes the row on 30 without the
/// lock, from `M` as it was before the `b` page.
#[tokio::test]
async fn r19_an_a_rederive_whose_row_moved_onto_a_held_key_retries() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        d.stage_recomputes("public.a", &["1"]).await;
        let rederive = d.seal().await;
        write(&d, "insert into public.b values (4, 30, 300, 'thirty')").await;
        let insert = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive a",
                &[(PausePoint::BeforeJoin1Lock, kind.target())],
            )
            .await;
        page.reached(PausePoint::BeforeJoin1Lock).await;
        write(&d, "update public.a set k = 30 where id = 1").await;
        let mut held = d
            .drain_frozen(insert, "b", &[(PausePoint::BeforeCommit, kind.target())])
            .await;
        let reached = held.reached(PausePoint::BeforeCommit).await;
        d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
        let blocked = queued_or_finished(&d, reached.backend_pid, &page).await;
        d.release(&mut held, PausePoint::BeforeCommit).await;
        held.finish().await;
        let result = page.finish_result().await;
        if !blocked {
            ran_through(&mut d, kind, result, moves).await;
        }
        result.expect("the Re-derive page");
        assert_eq!(
            trellis::dev::interleave::join_key_moves(),
            moves + 1,
            "the Re-derive's read found a key it hadn't locked"
        );
        assert_eq!(total(&d, kind, 1).await, Some(1 + 300), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// `rd_latesnap` on `M`: a `b` change that commits after a `b` Re-derive's
/// read statement is not in the row it wrote, so it must not be in the
/// entry's basis (`join1_snapshot_after_read`): its Apply, drained after the
/// Re-derive, applies and fans out.
#[tokio::test]
async fn a_b_change_between_a_b_rederives_read_and_its_snapshot_is_applied() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        let label = m_label(&d, kind).await;
        d.stage_recomputes("public.b", &["1"]).await;
        let rederive = d.seal().await;
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive b",
                &[(PausePoint::AfterRederiveRead, &label)],
            )
            .await;
        page.reached(PausePoint::AfterRederiveRead).await;
        write(&d, "update public.b set w = 500 where id = 1").await;
        let change = d.seal().await;
        d.release(&mut page, PausePoint::AfterRederiveRead).await;
        page.finish().await;
        d.drain(change, "change").await;
        assert_eq!(total(&d, kind, 1).await, Some(1 + 500), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// A page re-deriving rows of both sides reads each side in its own
/// statement, and each read's snapshot is the basis of its own side's
/// entries only. Changes of both rows that commit between the `a` read and
/// the `b` read are in the `b` read's row and snapshot but not the `a`
/// read's: drained after the page, the `a` change applies and the `b`
/// change is refused, its row being in `M` already.
#[tokio::test]
async fn changes_between_the_two_reads_of_a_page_rederiving_both_sides_apply() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        d.stage_recomputes("public.a", &["1"]).await;
        d.stage_recomputes("public.b", &["1"]).await;
        let rederive = d.seal().await;
        // The `a` read's pause point carries the target's label, the `b`
        // read's `M`'s, so the page stops between its two reads.
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive both",
                &[(PausePoint::AfterRederiveRead, kind.target())],
            )
            .await;
        page.reached(PausePoint::AfterRederiveRead).await;
        write(
            &d,
            "update public.a set x = 7 where id = 1; update public.b set w = 700 where id = 1",
        )
        .await;
        let change = d.seal().await;
        d.release(&mut page, PausePoint::AfterRederiveRead).await;
        page.finish().await;
        assert_eq!(
            total(&d, kind, 1).await,
            Some(1 + 700),
            "{kind:?}: the a read predates both changes, the b read follows them"
        );
        d.drain(change, "change").await;
        assert_eq!(total(&d, kind, 1).await, Some(7 + 700), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// A Re-derive of either side's rows that finds them as their entries hold
/// them writes no target row: its fan-out recomputes the `b` row's key, and
/// the upsert leaves each row whose fields are unchanged as it is.
#[tokio::test]
async fn a_rederive_that_changes_nothing_writes_no_target_row() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        let versions = format!("select id, xmin::text from {} order by id", kind.target());
        let before = d.rows(&versions).await;
        d.stage_recomputes("public.b", &["1", "2", "3"]).await;
        d.stage_recomputes("public.a", &["1", "2", "3", "4"]).await;
        let batch = d.seal().await;
        d.drain(batch, "rederive").await;
        assert_eq!(d.rows(&versions).await, before, "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// R17 on `M`: a `b` Re-derive read the row after two changes, then the
/// older change's Apply arrives. The entry's basis saw it, so it is refused
/// (`join1_lsn_only`) and the fan-out keeps the re-derived `w`.
#[tokio::test]
async fn r17_a_stale_b_apply_after_a_b_rederive_is_refused() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        write(&d, "update public.b set w = 110 where id = 1").await;
        let older = d.seal().await;
        write(&d, "update public.b set w = 120 where id = 1").await;
        d.stage_recomputes("public.b", &["1"]).await;
        let rederive = d.seal().await;
        d.drain(rederive, "rederive").await;
        assert_eq!(total(&d, kind, 1).await, Some(1 + 120));
        d.drain(older, "older").await;
        assert_eq!(total(&d, kind, 1).await, Some(1 + 120), "{kind:?}");
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// One writer of a Re-derive family: a Re-derive of `table`'s `keys`, a
/// captured change, or both in one page.
#[derive(Debug, Clone, Copy)]
enum Writer {
    Rederive(&'static str, &'static [&'static str]),
    Change(&'static str),
    Both(&'static str, &'static [&'static str], &'static str),
}

impl Writer {
    async fn seal(self, d: &mut Driver) -> i64 {
        match self {
            Writer::Rederive(table, keys) => d.stage_recomputes(table, keys).await,
            Writer::Change(sql) => write(d, sql).await,
            Writer::Both(table, keys, sql) => {
                write(d, sql).await;
                d.stage_recomputes(table, keys).await;
            }
        }
        d.seal().await
    }
}

/// One Re-derive family: two writers on a key they share, each in its own
/// batch, for both join kinds, with either one's page frozen at its commit
/// while the other queues.
async fn rederive_family(first: Writer, second: Writer) {
    for kind in KINDS {
        for flip in [false, true] {
            let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
            let one = first.seal(&mut d).await;
            let two = second.seal(&mut d).await;
            if flip {
                race(&mut d, kind, two, one).await;
            } else {
                race(&mut d, kind, one, two).await;
            }
        }
    }
}

/// Re-derives of either side against a change of the same row, of the other
/// side on its key, a key move onto or off it, another Re-derive and a mixed
/// page: each pair queues on a shared `J` row, nothing deadlocks, and the
/// target is the live join whichever page commits first.
#[tokio::test]
async fn r_rederives_race_changes_of_either_side() {
    let _serial = serial().await;
    const B1: Writer = Writer::Rederive("public.b", &["1"]);
    const A1: Writer = Writer::Rederive("public.a", &["1"]);
    for (first, second) in [
        // A `b` Re-derive against an `a` insert on its key, an update of the
        // same row, and the row's move off the key.
        (
            B1,
            Writer::Change("insert into public.a values (5, 10, 5, 'five')"),
        ),
        (
            B1,
            Writer::Change("update public.b set w = 101 where id = 1"),
        ),
        (
            B1,
            Writer::Change("update public.b set k = 50 where id = 1"),
        ),
        // An `a` Re-derive against a `b` update of its key, a `b` row moving
        // onto its key, and its own move.
        (
            A1,
            Writer::Change("update public.b set w = 102 where id = 1"),
        ),
        (
            Writer::Rederive("public.a", &["3"]),
            Writer::Change("update public.b set k = 30 where id = 3"),
        ),
        (
            A1,
            Writer::Change("update public.a set k = 20 where id = 1"),
        ),
        // Re-derives of both sides on one key, and a page with both against
        // a `b` change of the key.
        (A1, B1),
        (
            Writer::Both("public.b", &["1"], "update public.a set x = 9 where id = 1"),
            Writer::Change("update public.b set w = 103 where id = 1"),
        ),
    ] {
        rederive_family(first, second).await;
    }
}

/// N-family: a row with a `NULL` join key has no `J` row, so only its entry
/// lock orders a Re-derive of it against an Apply (`join1_no_entry_lock`).
/// The Re-derive is frozen after its read; the Apply's change commits after
/// that read and its page queues on the entry, then applies over the
/// re-derived state. Planted, the Apply runs first and the Re-derive then
/// writes the older row over it. For both sides: `a` row 4 and a `b` row on
/// a `NULL` key.
#[tokio::test]
async fn n_a_rederive_and_an_apply_of_a_null_keyed_row_queue_on_its_entry() {
    let _serial = serial().await;
    for kind in KINDS {
        for (table, change) in [
            ("public.a", "update public.a set x = 44 where id = 4"),
            ("public.b", "update public.b set w = 4400 where id = 4"),
        ] {
            let mut d = start_kinds(
                &[kind],
                A_ROWS,
                &format!("{B_ROWS}, (4, null, 4000, 'none')"),
            )
            .await;
            let label = if table == "public.a" {
                kind.target().to_string()
            } else {
                m_label(&d, kind).await
            };
            d.stage_recomputes(table, &["4"]).await;
            let rederive = d.seal().await;
            let mut page = d
                .drain_frozen(
                    rederive,
                    "rederive",
                    &[(PausePoint::AfterRederiveRead, &label)],
                )
                .await;
            let reached = page.reached(PausePoint::AfterRederiveRead).await;
            write(&d, change).await;
            let batch = d.seal().await;
            let moves = trellis::dev::interleave::join_key_moves();
            let apply = d.drain_frozen(batch, "apply", &[]).await;
            let blocked = queued_or_finished(&d, reached.backend_pid, &apply).await;
            d.release(&mut page, PausePoint::AfterRederiveRead).await;
            page.finish().await;
            let result = apply.finish_result().await;
            if !blocked {
                ran_through(&mut d, kind, result, moves).await;
            }
            result.expect("the Apply page");
            assert_oracle(&mut d, &[kind]).await;
        }
    }
}

/// N-family: Re-derives move rows of either side onto and off a `NULL` key
/// (changes capture never saw), and re-derive a row that stays on one.
#[tokio::test]
async fn n_rederives_move_rows_onto_and_off_a_null_key() {
    let _serial = serial().await;
    let mut d = start(A_ROWS, &format!("{B_ROWS}, (4, null, 4000, 'none')")).await;
    write_uncaptured(&d, "update public.a set k = 10 where id = 4").await;
    write_uncaptured(&d, "update public.a set k = null where id = 2").await;
    write_uncaptured_on(&d, "public.b", "update public.b set k = 30 where id = 4").await;
    write_uncaptured_on(&d, "public.b", "update public.b set k = null where id = 1").await;
    d.stage_recomputes("public.a", &["2", "4"]).await;
    d.stage_recomputes("public.b", &["1", "3", "4"]).await;
    let batch = d.seal().await;
    d.drain(batch, "rederive").await;
    assert_oracle(&mut d, &KINDS).await;
    // A row that stays on a `NULL` key re-derives to the same state.
    write_uncaptured_on(&d, "public.b", "update public.b set w = 1 where id = 1").await;
    d.stage_recomputes("public.a", &["2"]).await;
    d.stage_recomputes("public.b", &["1"]).await;
    let batch = d.seal().await;
    d.drain(batch, "again").await;
    assert_oracle(&mut d, &KINDS).await;
}

/// A `b` side that is another transform's 1-1 target, joined on its primary
/// key: the upstream's writes reach the join through the target-mutation
/// seam's image feed as Applies with images, which the `b` page applies like
/// captured ones (an update, a delete, a re-insert), and an `a` move after
/// them reads the new `M`. A Re-derive of the upstream's rows (a seam
/// `Recompute`) reads the target live.
#[tokio::test]
async fn a_chained_b_side_applies_the_seams_images() {
    let _serial = serial().await;
    set_enabled(true);
    let mut d = Driver::start(
        "create table public.a (id integer primary key, k integer, x integer); \
         create table public.bsrc (id integer primary key, w integer); \
         insert into public.a values (1, 10, 1), (2, 10, 2), (3, 20, 3); \
         insert into public.bsrc values (10, 100), (20, 200)",
        &[("w", int4())],
        &["TRANSFORM tb FROM public.bsrc SELECT w AS w"],
        &["public.bsrc"],
    )
    .await;
    // The join can only be defined over a live upstream.
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM jc FROM public.a LEFT JOIN public.tb ON a.k = tb.id \
         SELECT a.x + COALESCE(tb.w, 0) AS total",
        &std::collections::HashMap::from([("total".to_string(), int4())]),
        "public",
    )
    .await
    .expect("define the join over the 1-1 target");
    let catalog = trellis::capture::columns::load_catalog(&d.ctl)
        .await
        .expect("load the catalog");
    let spec = trellis::capture::columns::capture_spec(&d.ctl, &catalog, "public.a")
        .await
        .expect("capture spec");
    match trellis::capture::install::reconcile(
        &mut d.ctl,
        trellis::config::DEFAULT_SCHEMA,
        &spec,
        None,
    )
    .await
    .expect("capture a")
    {
        trellis::capture::install::Progress::Done(_) => {}
        trellis::capture::install::Progress::Waiting(wait) => panic!("capture waited: {wait}"),
    }
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    for sql in [
        "update public.bsrc set w = 111 where id = 10",
        "delete from public.bsrc where id = 20",
        "insert into public.bsrc values (20, 222)",
        "update public.a set k = 20 where id = 1",
    ] {
        step(&mut d, sql).await;
        d.settle().await;
        let actual = d.rows("select id, total from public.jc order by id").await;
        let expected = d
            .rows(
                "select a.id, a.x + coalesce(b.w, 0) from public.a a \
                 left join public.bsrc b on a.k = b.id order by a.id",
            )
            .await;
        assert_eq!(actual, expected, "after {sql}");
        let status = d
            .rows("select status::text from transform_definitions order by id")
            .await;
        assert_eq!(status, ["(live)", "(live)"], "after {sql}");
    }
    d.stage_recomputes("public.tb", &["10", "20"]).await;
    let batch = d.seal().await;
    d.drain(batch, "rederive tb").await;
    d.settle().await;
    let actual = d.rows("select id, total from public.jc order by id").await;
    let expected = d
        .rows(
            "select a.id, a.x + coalesce(b.w, 0) from public.a a \
             left join public.bsrc b on a.k = b.id order by a.id",
        )
        .await;
    assert_eq!(actual, expected, "after a Re-derive of tb");
    let status = d
        .rows("select status::text from transform_definitions order by id")
        .await;
    assert_eq!(status, ["(live)", "(live)"], "after a Re-derive of tb");
}

/// A text join key with a collation: `K`, the held-key checks and the
/// fan-out cast each side's key to `J`'s type and collation before comparing
/// its text.
#[tokio::test]
async fn a_collated_text_key_applies_both_sides() {
    let _serial = serial().await;
    set_enabled(true);
    let mut d = Driver::start(
        "create table public.c (id integer primary key, k text collate \"C\", x integer); \
         create table public.e (id integer primary key, k text collate \"C\", w integer); \
         create unique index on public.e (k); \
         insert into public.c values (1, 'p', 1), (2, 'p', 2), (3, 'q', 3); \
         insert into public.e values (1, 'p', 100), (2, 'q', 200)",
        &[("id", int4()), ("k", ValueType::Text), ("x", int4())],
        &[
            "TRANSFORM ct FROM public.c LEFT JOIN public.e ON c.k = e.k \
           SELECT c.x + COALESCE(e.w, 0) AS total",
        ],
        &["public.c", "public.e"],
    )
    .await;
    for sql in [
        "update public.e set w = 111 where id = 1",
        "update public.c set k = 'q' where id = 1",
        "update public.e set k = 'r' where id = 2",
        "update public.e set k = 'q', w = 150 where id = 1",
    ] {
        step(&mut d, sql).await;
        d.settle().await;
        let actual = d.rows("select id, total from public.ct order by id").await;
        let expected = d
            .rows(
                "select c.id, c.x + coalesce(e.w, 0) from public.c c \
                 left join public.e e on c.k = e.k order by c.id",
            )
            .await;
        assert_eq!(actual, expected, "after {sql}");
        let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), "public.ct")
            .await
            .expect("self_check");
        assert!(diverging.is_empty(), "after {sql}: {diverging:?}");
    }
}

/// The two sides keyed differently (`a` by text, `b` by `bigint`), with
/// columns named like the aliases the page's statements give their
/// relations (`r`, `s`, `b`, `m`, `l`, `doc`, `img`, `k0`). A page whose
/// first change of the target is a `b` change still writes the target by
/// `a`'s key, in a `b`-only page, a mixed page and a `b` key move, on both
/// join kinds. Then Re-derives of either side, alone and in one page, repair
/// changes capture never saw.
#[tokio::test]
async fn sides_keyed_differently_with_alias_named_columns_apply() {
    let _serial = serial().await;
    set_enabled(true);
    // `Driver::rows` reads its rows as `r`, so the checks below name their
    // columns apart from it.
    let mut d = Driver::start(
        "create table public.o (order_no text primary key, s integer, r integer, l text); \
         create table public.cu (cust_id bigint primary key, m integer, b integer, doc text, \
             img text, k0 integer); \
         create unique index on public.cu (m); \
         insert into public.o values ('A1', 1, 1, 'x'), ('A2', 1, 2, 'y'), ('A3', 2, 3, 'z'), \
             ('A4', 3, 4, 'w'); \
         insert into public.cu values (10, 1, 100, 'd1', 'i1', 7), (20, 2, 200, 'd2', 'i2', 8)",
        &[
            ("order_no", ValueType::Text),
            ("s", int4()),
            ("r", int4()),
            ("l", ValueType::Text),
        ],
        &[
            "TRANSFORM ol FROM public.o LEFT JOIN public.cu ON o.s = cu.m \
             SELECT o.r + COALESCE(cu.b, 0) AS s, cu.doc AS b, cu.img AS r, cu.k0 AS m, o.l AS l",
            "TRANSFORM oi FROM public.o INNER JOIN public.cu ON o.s = cu.m \
             SELECT o.r + COALESCE(cu.b, 0) AS s, cu.doc AS b, cu.img AS r, cu.k0 AS m, o.l AS l",
        ],
        &["public.o", "public.cu"],
    )
    .await;
    for sql in [
        "update public.cu set b = 111, doc = 'd1b' where cust_id = 10",
        "update public.o set r = 5 where order_no = 'A1'; \
         update public.cu set b = 222, img = 'i2b' where cust_id = 20",
        "update public.cu set m = 3, k0 = 9 where cust_id = 20; \
         update public.o set s = 2 where order_no = 'A2'",
        "insert into public.cu values (30, 2, 300, 'd3', 'i3', 1)",
        "delete from public.cu where cust_id = 10",
    ] {
        step(&mut d, sql).await;
        d.settle().await;
        assert_ordered_join(&d, sql).await;
    }
    for (table, sql, keys) in [
        (
            "public.cu",
            "update public.cu set b = 333, doc = 'd2c', k0 = 4 where cust_id = 20",
            &["20"][..],
        ),
        (
            "public.cu",
            "update public.cu set m = 1, img = 'i3b' where cust_id = 30",
            &["30"][..],
        ),
        (
            "public.o",
            "update public.o set s = 1, r = 6, l = 'v' where order_no = 'A4'",
            &["A4"][..],
        ),
    ] {
        write_uncaptured_on(&d, table, sql).await;
        d.stage_recomputes(table, keys).await;
        let batch = d.seal().await;
        d.drain(batch, "rederive").await;
        d.settle().await;
        assert_ordered_join(&d, sql).await;
    }
    // Both sides' Re-derives in one page.
    write_uncaptured_on(
        &d,
        "public.cu",
        "update public.cu set b = 444 where cust_id = 30",
    )
    .await;
    write_uncaptured_on(
        &d,
        "public.o",
        "update public.o set r = 7 where order_no = 'A1'",
    )
    .await;
    d.stage_recomputes("public.cu", &["20", "30"]).await;
    d.stage_recomputes("public.o", &["A1", "A3"]).await;
    let batch = d.seal().await;
    d.drain(batch, "rederive both").await;
    d.settle().await;
    assert_ordered_join(&d, "a mixed Re-derive page").await;
}

/// [`sides_keyed_differently_with_alias_named_columns_apply`]'s check: each
/// target equals its live join and self_check finds nothing, after `step`.
async fn assert_ordered_join(d: &Driver, step: &str) {
    for (target, kind) in [("public.ol", "left"), ("public.oi", "inner")] {
        let actual = d
            .rows(&format!(
                "select order_no, s as c1, b as c2, r as c3, m as c4, l as c5 \
                 from {target} order by order_no"
            ))
            .await;
        let expected = d
            .rows(&format!(
                "select o.order_no, o.r + coalesce(cu.b, 0) as c1, cu.doc as c2, cu.img as c3, \
                 cu.k0 as c4, o.l as c5 \
                 from public.o o {kind} join public.cu cu on o.s = cu.m order by o.order_no"
            ))
            .await;
        assert_eq!(actual, expected, "{target} after {step}");
        let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), target)
            .await
            .expect("self_check");
        assert!(diverging.is_empty(), "{target} after {step}: {diverging:?}");
    }
}

// ------------------------------------------------------------- truncates

/// Runs the builds a `TRUNCATE` of `b` started, then settles the pipeline
/// and asserts each of `kinds`: the target equals the live join, `R` equals
/// `a` and `M` equals `b`.
async fn assert_rebuilt(d: &mut Driver, kinds: &[Kind]) {
    trellis::staging::build::settle_builds_with(d.pool(), 3).await;
    assert_oracle(d, kinds).await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
}

/// The status of `kind`'s definition and the number of build jobs it has
/// left.
async fn build_state(d: &Driver, kind: Kind) -> (String, i64) {
    let row = d
        .ctl
        .query_one(
            "select d.status::text, \
                    (select count(*) from backfill_chunks c \
                     where c.definition_id = d.id and not c.done) \
             from transform_definitions d where d.target_table = $1",
            &[&kind.target()],
        )
        .await
        .expect("read the definition's status");
    (row.get(0), row.get(1))
}

/// A `TRUNCATE a` empties `R` and the target in its page, raises `R`'s floor
/// and leaves `M` and `J` as they were; the definition keeps reading `live`,
/// and the rows inserted after it apply.
#[tokio::test]
async fn a_truncate_of_a_empties_r_and_the_target_and_later_inserts_apply() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        let id = definition_id(&d, kind.target()).await;
        let schema = trellis::config::DEFAULT_SCHEMA;
        let j_before = d
            .rows(&format!("select k from {schema}.join1_{id}_j order by 1"))
            .await;
        step(&mut d, "truncate public.a").await;
        assert_eq!(
            d.rows(&format!("select count(*) from {}", kind.target()))
                .await,
            ["(0)"],
            "{kind:?}: the target is empty"
        );
        assert_eq!(
            d.rows(&format!("select count(*) from {schema}.join1_{id}_r"))
                .await,
            ["(0)"],
            "{kind:?}: R is empty"
        );
        assert_eq!(
            d.rows(&format!("select count(*) from {schema}.join1_{id}_m"))
                .await,
            ["(3)"],
            "{kind:?}: M is untouched"
        );
        assert_eq!(
            d.rows(&format!("select k from {schema}.join1_{id}_j order by 1"))
                .await,
            j_before,
            "{kind:?}: J keeps its rows"
        );
        assert_eq!(
            d.rows(&format!(
                "select count(*) from ledger_truncate_floor \
                 where target_table = '\"{schema}\".\"join1_{id}_r\"'"
            ))
            .await,
            ["(1)"],
            "{kind:?}: R's floor rose"
        );
        assert_eq!(
            build_state(&d, kind).await,
            ("live".to_string(), 0),
            "{kind:?}: a TRUNCATE of a starts no rebuild"
        );
        step(&mut d, "insert into public.a values (7, 10, 7, 'seven')").await;
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// After a `TRUNCATE a`, a change of `b` fans out to no row: `R` holds none
/// of the `a` rows that are gone, so the target stays empty.
#[tokio::test]
async fn a_truncate_of_a_empties_r_and_a_later_b_change_writes_nothing() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        step(&mut d, "truncate public.a").await;
        step(&mut d, "update public.b set w = 999 where id = 1").await;
        assert_eq!(
            d.rows(&format!("select count(*) from {}", kind.target()))
                .await,
            ["(0)"],
            "{kind:?}: a b change resurrected the rows of a rows that are gone"
        );
        assert_oracle(&mut d, &[kind]).await;
    }
}

/// A `TRUNCATE b` empties `M` and raises its floor in its page, and starts a
/// rebuild of the target: the definition reads `backfilling` with the plan
/// and sweep jobs queued, and keeps applying. Its rows keep the `b` side they
/// held until the rebuild reaches them; then a `LEFT` row's is `NULL` and an
/// `INNER` row is gone.
#[tokio::test]
async fn a_truncate_of_b_rebuilds_the_target() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        let id = definition_id(&d, kind.target()).await;
        let schema = trellis::config::DEFAULT_SCHEMA;
        step(&mut d, "truncate public.b").await;
        assert_eq!(
            d.rows(&format!("select count(*) from {schema}.join1_{id}_m"))
                .await,
            ["(0)"],
            "{kind:?}: M is empty"
        );
        assert_eq!(
            d.rows(&format!(
                "select count(*) from ledger_truncate_floor \
                 where target_table = '\"{schema}\".\"join1_{id}_m\"'"
            ))
            .await,
            ["(1)"],
            "{kind:?}: M's floor rose"
        );
        assert_eq!(
            build_state(&d, kind).await,
            ("backfilling".to_string(), 2),
            "{kind:?}: a plan job and a sweep job are queued"
        );
        assert_eq!(
            d.rows(&format!(
                "select (build_seg is not null)::text from transform_definitions \
                 where id = {id}"
            ))
            .await,
            ["(true)"],
            "{kind:?}: the rebuild recorded its segment"
        );
        // The target still holds the old `b` side.
        let stale = d
            .rows(&format!(
                "select count(*) from {} where tag is not null",
                kind.target()
            ))
            .await;
        assert_eq!(stale, ["(2)"], "{kind:?}: the rows wait for the rebuild");
        assert_rebuilt(&mut d, &[kind]).await;
        assert_eq!(
            build_state(&d, kind).await,
            ("live".to_string(), 0),
            "{kind:?}: the rebuild finished"
        );
        let expected = match kind {
            Kind::Left => ["(4)"],
            Kind::Inner => ["(0)"],
        };
        assert_eq!(
            d.rows(&format!("select count(*) from {}", kind.target()))
                .await,
            expected
        );
    }
}

/// After a `TRUNCATE b` and before the rebuild reaches any row, an `a` change
/// reads the `M` the truncate left, which is empty: a new `a` row on a key
/// whose `b` row is gone has no `b` side.
#[tokio::test]
async fn a_truncate_of_b_empties_m_before_the_rebuild_runs() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        step(&mut d, "truncate public.b").await;
        step(&mut d, "insert into public.a values (5, 10, 5, 'five')").await;
        assert_eq!(
            d.rows(&format!(
                "select count(*) from {} where id = 5 and tag is not null",
                kind.target()
            ))
            .await,
            ["(0)"],
            "{kind:?}: the new row joined a b row that is gone"
        );
        assert_rebuilt(&mut d, &[kind]).await;
    }
}

/// A `b` row inserted after the `TRUNCATE b` in the same transaction is in
/// `M` once the truncate's page has applied it, and the rebuild keeps it.
#[tokio::test]
async fn a_truncate_of_b_and_a_later_insert_in_one_transaction() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        step(
            &mut d,
            "begin; truncate public.b; \
             insert into public.b values (4, 10, 401, 'ten2'), (5, null, 1, 'none'); commit",
        )
        .await;
        assert_eq!(
            d.rows(&format!(
                "select count(*) from {}.join1_{}_m where not __tombstone",
                trellis::config::DEFAULT_SCHEMA,
                definition_id(&d, kind.target()).await
            ))
            .await,
            ["(2)"],
            "{kind:?}: the later inserts are in M"
        );
        assert_rebuilt(&mut d, &[kind]).await;
    }
}

/// The #1375 truncate families (T1 to T8, named there by the group "truncates
/// of either side or both, alone or with later inserts, under the barrier"),
/// numbered here in the order of [`TRUNCATE_RACES`]: a `TRUNCATE` of either
/// side against a page of the other side (T1, T2) and of its own (T3, T4), a
/// `TRUNCATE` and the inserts after it in one transaction against a page of
/// the other side (T5, T6), and both sides truncated and refilled (T7). T8 is
/// [`t8_a_stale_page_after_the_truncate_is_refused`].
///
/// Each pair is two batches, sealed in order, and drains in both orders, the
/// first frozen at its commit: the second must queue on the pair's lock row,
/// which the truncate's page holds `FOR UPDATE` and every other page `FOR
/// SHARE`, and then the target equals the live join.
const TRUNCATE_RACES: [(&str, &str, &str); 8] = [
    (
        "t1 truncate a vs b",
        "truncate public.a",
        "update public.b set w = 999 where id = 2",
    ),
    (
        // A page of `b` that holds a `NULL` key reads no `R` entry, so it
        // meets the truncate's page on the lock row alone.
        "t1 truncate a vs a NULL-keyed b change",
        "truncate public.a",
        "insert into public.b values (4, null, 1, 'none')",
    ),
    (
        "t2 truncate b vs a",
        "truncate public.b",
        "insert into public.a values (5, 20, 5, 'five')",
    ),
    (
        "t3 a vs truncate a",
        "update public.a set x = 99 where id = 1; \
         insert into public.a values (5, 30, 5, 'five')",
        "truncate public.a",
    ),
    (
        "t4 b vs truncate b",
        "insert into public.b values (4, 30, 400, 'thirty')",
        "truncate public.b",
    ),
    (
        "t5 truncate a and insert vs b",
        "begin; truncate public.a; \
         insert into public.a values (5, 10, 5, 'five'), (6, null, 6, 'six'); commit",
        "update public.b set w = 999 where id = 1",
    ),
    (
        "t6 truncate b and insert vs a",
        "begin; truncate public.b; \
         insert into public.b values (4, 10, 401, 'ten2'), (5, null, 1, 'none'); commit",
        "insert into public.a values (5, 20, 5, 'five')",
    ),
    (
        "t7 both truncated and refilled vs b",
        "begin; truncate public.a; truncate public.b; \
         insert into public.a values (5, 10, 5, 'five'); \
         insert into public.b values (4, 10, 400, 'ten3'); commit",
        "insert into public.b values (6, 50, 1, 'fifty')",
    ),
];

#[tokio::test]
async fn t1_to_t7_a_truncate_races_a_page_of_either_side() {
    let _serial = serial().await;
    for kind in KINDS {
        for (name, x, y) in TRUNCATE_RACES {
            for first_x in [true, false] {
                let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
                write(&d, x).await;
                let sealed_x = d.seal().await;
                write(&d, y).await;
                let sealed_y = d.seal().await;
                let (first, second) = if first_x {
                    (sealed_x, sealed_y)
                } else {
                    (sealed_y, sealed_x)
                };
                race_rebuilt(&mut d, kind, name, first, second).await;
            }
        }
    }
}

/// [`race`] for batches that may hold a `TRUNCATE` of `b`: the builds it
/// started run before the oracle.
async fn race_rebuilt(d: &mut Driver, kind: Kind, name: &str, first: i64, second: i64) {
    let mut held = d
        .drain_frozen(first, "first", &[(PausePoint::BeforeCommit, kind.target())])
        .await;
    let reached = held.reached(PausePoint::BeforeCommit).await;
    let queued = d.drain_frozen(second, "second", &[]).await;
    let blocked = queued_or_finished(d, reached.backend_pid, &queued).await;
    d.release(&mut held, PausePoint::BeforeCommit).await;
    held.finish().await;
    let result = queued.finish_result().await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
    assert!(
        blocked,
        "{kind:?} {name}: the second page ran without queueing on the first's lock row: \
         {result:?}"
    );
    result.expect("the second page");
    assert_rebuilt(d, &[kind]).await;
}

/// T8: a page of the truncated side from before the truncate, drained after
/// the truncate's page (the barrier never lets it; the driver claims it out of
/// order on purpose), is refused by the side's floor, and the ledger and the
/// target hold nothing of it.
#[tokio::test]
async fn t8_a_stale_page_after_the_truncate_is_refused() {
    let _serial = serial().await;
    for kind in KINDS {
        for table in ["public.a", "public.b"] {
            let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
            let stale = match table {
                "public.a" => {
                    "insert into public.a values (9, 20, 9, 'nine'); \
                     update public.a set x = 99 where id = 1"
                }
                _ => {
                    "insert into public.b values (9, 30, 900, 'thirty'); \
                     update public.b set w = 99 where id = 1"
                }
            };
            write(&d, stale).await;
            let earlier = d.seal().await;
            write(&d, &format!("truncate {table}")).await;
            let truncate = d.seal().await;
            d.drain(truncate, "truncate").await;
            d.drain(earlier, "stale").await;
            let id = definition_id(&d, kind.target()).await;
            let ledger = if table == "public.a" { "r" } else { "m" };
            assert_eq!(
                d.rows(&format!(
                    "select count(*) from {}.join1_{id}_{ledger} where not __tombstone",
                    trellis::config::DEFAULT_SCHEMA
                ))
                .await,
                ["(0)"],
                "{kind:?} {table}: the stale page wrote an entry"
            );
            if table == "public.a" {
                assert_eq!(
                    d.rows(&format!("select count(*) from {}", kind.target()))
                        .await,
                    ["(0)"],
                    "{kind:?}: the stale page wrote a target row"
                );
            }
            assert_rebuilt(&mut d, &[kind]).await;
        }
    }
}

/// The barrier: a `TRUNCATE b`, then a later batch with an insert into `a` and
/// a change of `b` (a mixed page). Nothing past the truncate's batch is
/// claimable until it drains, so the mixed page can't apply its insert first
/// and have the rebuild the truncate starts treat it as older than it is.
#[tokio::test]
async fn a_batch_after_a_truncate_waits_for_it() {
    let _serial = serial().await;
    let mut d = start_kinds(&[Kind::Left], A_ROWS, B_ROWS).await;
    write(&d, "truncate public.b").await;
    let truncate = d.seal().await;
    write(
        &d,
        "insert into public.a values (5, 20, 5, 'five'); \
         insert into public.b values (4, 20, 1, 'again')",
    )
    .await;
    let mixed = d.seal().await;
    let claimable = || async {
        trellis::staging::apply::next_claimable_segments(&d.ctl, 1_000_000)
            .await
            .expect("next claimable segments")
    };
    assert_eq!(claimable().await, [truncate], "the mixed page waits");
    d.drain(truncate, "truncate").await;
    assert_eq!(claimable().await, [mixed]);
    d.drain(mixed, "mixed").await;
    assert_rebuilt(&mut d, &[Kind::Left]).await;
    assert_eq!(
        d.rows("select count(*) from public.b").await,
        ["(1)"],
        "the insert survived"
    );
}

/// A rebuild's start takes the definition's row and then the pair's lock row.
/// A `TRUNCATE b` page takes them in that order too, so one that arrives while
/// a rebuild is starting queues on the definition's row and the two never wait
/// for each other. The start is played by hand: its transaction holds the row.
#[tokio::test]
async fn a_truncate_of_b_page_and_a_rebuild_start_take_the_definition_row_first() {
    let _serial = serial().await;
    let mut d = start_kinds(&[Kind::Left], A_ROWS, B_ROWS).await;
    let id = definition_id(&d, Kind::Left.target()).await;
    write(&d, "truncate public.b").await;
    let truncate = d.seal().await;
    let mut start = d.user().await;
    let txn = start.transaction().await.expect("begin");
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&id],
    )
    .await
    .expect("the definition row");
    let page = d.drain_frozen(truncate, "truncate", &[]).await;
    // The page queues on the definition's row, holding nothing else.
    let pid: i32 = txn
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    assert!(
        queued_or_finished(&d, pid, &page).await,
        "the page didn't queue on the definition's row"
    );
    // The start goes on to the lock row, which the queued page doesn't hold.
    txn.execute(
        &format!(
            "select 1 from {}.join1_{id}_l for update",
            trellis::config::DEFAULT_SCHEMA
        ),
        &[],
    )
    .await
    .expect("the lock row, with no deadlock");
    txn.commit().await.expect("commit the start");
    page.finish().await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
    assert_rebuilt(&mut d, &[Kind::Left]).await;
}

/// A `TRUNCATE b` of two 1-1 `JOIN`s over `b` whose ids run against their
/// target names (`jl` is defined first, `ji` sorts first), against a rebuild
/// start of both, which takes their definitions' rows in id order and then
/// their lock rows. The page takes both definitions' rows in id order before
/// either lock row, so it queues on `jl`'s holding nothing of either pair.
/// Taking each definition's row as it reached that target, it would hold
/// `ji`'s row and lock row while it queued, and the start's next lock would
/// close the cycle.
#[tokio::test]
async fn a_truncate_of_b_page_of_two_joins_takes_their_definition_rows_in_id_order() {
    let _serial = serial().await;
    let mut d = start_kinds(&[Kind::Left, Kind::Inner], A_ROWS, B_ROWS).await;
    let left = definition_id(&d, Kind::Left.target()).await;
    let inner = definition_id(&d, Kind::Inner.target()).await;
    assert!(left < inner, "jl is defined first");
    write(&d, "truncate public.b").await;
    let truncate = d.seal().await;
    let mut start = d.user().await;
    let txn = start.transaction().await.expect("begin");
    let lock_definition = |id: i64| {
        let txn = &txn;
        async move {
            txn.execute(
                "select 1 from transform_definitions where id = $1 for update",
                &[&id],
            )
            .await
        }
    };
    lock_definition(left).await.expect("jl's definition row");
    let page = d.drain_frozen(truncate, "truncate", &[]).await;
    let pid: i32 = txn
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    assert!(
        queued_or_finished(&d, pid, &page).await,
        "the page didn't queue on jl's definition row"
    );
    lock_definition(inner)
        .await
        .expect("ji's definition row, with no deadlock");
    for id in [inner, left] {
        txn.execute(
            &format!(
                "select 1 from {}.join1_{id}_l for update",
                trellis::config::DEFAULT_SCHEMA
            ),
            &[],
        )
        .await
        .expect("the lock rows, with no deadlock");
    }
    txn.commit().await.expect("commit the start");
    page.finish().await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
    assert_rebuilt(&mut d, &[Kind::Left, Kind::Inner]).await;
}

/// A `TRUNCATE b` page against seal phase 1 sealing the segment after the
/// truncate's. Phase 1 updates the truncate's segment row (its `seal_step2`)
/// and then the segment pointer; the page marks its segment row drained and
/// stamps the rebuild's segment under a share lock on the pointer. The page
/// takes them in phase 1's order, its segment row first, so it queues on the
/// row holding no lock on the pointer, and phase 1 goes on. Phase 1 is played
/// by hand, once the page has claimed its segment.
#[tokio::test]
async fn a_truncate_of_b_page_takes_its_segment_row_before_the_segment_pointer() {
    let _serial = serial().await;
    let mut d = start_kinds(&[Kind::Left], A_ROWS, B_ROWS).await;
    write(&d, "truncate public.b").await;
    let truncate = d.seal().await;
    let mut page = d
        .drain_frozen(
            truncate,
            "truncate",
            &[(PausePoint::BeforeJoin1Recompute, Kind::Left.target())],
        )
        .await;
    page.reached(PausePoint::BeforeJoin1Recompute).await;
    let mut seal = d.user().await;
    let txn = seal.transaction().await.expect("begin");
    txn.execute(
        &format!(
            "update {}.segments set seal_step2 = seal_step2 where seg_seq = $1",
            trellis::config::DEFAULT_SCHEMA
        ),
        &[&truncate],
    )
    .await
    .expect("the truncate's segment row");
    d.release(&mut page, PausePoint::BeforeJoin1Recompute).await;
    let pid: i32 = txn
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    assert!(
        queued_or_finished(&d, pid, &page).await,
        "the page didn't queue on its segment row"
    );
    txn.execute(
        &format!(
            "update {}.segment_pointer set active_seq = active_seq",
            trellis::config::DEFAULT_SCHEMA
        ),
        &[],
    )
    .await
    .expect("the segment pointer, with no deadlock");
    txn.rollback().await.expect("roll phase 1 back");
    page.finish().await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
    assert_rebuilt(&mut d, &[Kind::Left]).await;
}
