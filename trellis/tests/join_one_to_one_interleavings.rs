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
//! Re-derives of `b` and truncates are #1398 and #1399's.

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
/// a change no entry will ever hear about. (Capture triggers are `ENABLE
/// ALWAYS`, so `session_replication_role = replica` doesn't skip them.)
async fn write_uncaptured(d: &Driver, sql: &str) {
    let toggle = |action: &str| {
        format!(
            "do $$ declare t text; begin \
                 for t in select tgname from pg_trigger \
                          where tgrelid = 'public.a'::regclass and not tgisinternal loop \
                     execute format('alter table public.a {action} trigger %I', t); \
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

/// A Re-derive of a `b` row halts the transform with the reason rather than
/// run a path that isn't built (#1398), and writes nothing: one staged by a
/// producer, and one the build-start rule makes of a `b` change whose page
/// predates the build.
#[tokio::test]
async fn a_rederive_of_a_b_row_halts_the_transform() {
    let _serial = serial().await;
    let mut d = start_kinds(&[Kind::Left], A_ROWS, B_ROWS).await;
    d.stage_recomputes("public.b", &["1"]).await;
    let batch = d.seal().await;
    d.drain(batch, "rederive b").await;
    let (status, reason) = paused_reason(&d, Kind::Left).await;
    assert_eq!(status, "paused");
    let reason = reason.expect("a reason");
    assert!(reason.contains("Re-derive of the joined table"), "{reason}");
    assert_eq!(total(&d, Kind::Left, 1).await, Some(1 + 100));

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
    trellis::staging::build::settle_builds(d.pool()).await;
    d.settle().await;
    let (status, reason) = paused_reason(&d, Kind::Left).await;
    assert_eq!(status, "paused");
    let reason = reason.expect("a reason");
    assert!(reason.contains("Re-derive of the joined table"), "{reason}");
}

/// A `b` side that is another transform's 1-1 target, joined on its primary
/// key: the upstream's writes reach the join through the target-mutation
/// seam's image feed as Applies with images, which the `b` page applies like
/// captured ones (an update, a delete, a re-insert), and an `a` move after
/// them reads the new `M`.
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
/// join kinds.
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
            assert_eq!(actual, expected, "{target} after {sql}");
            let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), target)
                .await
                .expect("self_check");
            assert!(diverging.is_empty(), "{target} after {sql}: {diverging:?}");
        }
    }
}
