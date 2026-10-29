//! Deterministic interleavings of the drain on 1-1 `JOIN` targets (ADR-0021,
//! epic #1392), ported from the #1375 model, for the `FROM` (`a`) side:
//! `a` changes, `J`, `R`, the recompute and the target write, and the
//! minimal build.
//!
//! Every test turns the dev knob on before its driver starts. Each scenario
//! seeds `a` and `b`, defines the transform (the minimal build fills `M`,
//! `R`, `J` and the target), writes `a` changes, seals them into batches and
//! drives the pages by hand (`support/drain_driver.rs`): one page frozen at a
//! [`PausePoint`] of its apply while another runs, which either queues behind
//! the frozen page (asserted with `pg_blocking_pids`) or runs through.
//! Nothing sleeps or polls for convergence. After each scenario the pipeline
//! settles and the target equals the live join
//! (`trellis::staging::one_to_one_join_diverging_keys`), and `R` equals `a`.
//!
//! A scenario runs on both join kinds, `LEFT` and `INNER`. A test is named
//! for the #1375 family it ports, where it has one:
//!
//! - `r17_…`: a stale Apply after a Re-derive (R17, the #392 shape).
//! - `n…`: `NULL` join keys (N1–N6).
//!
//! The `b` side's families (M1–M6, the fan-out), Re-derives of `b` and
//! truncates are #1397 to #1399's.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::integer::IntWidth;
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
        &["public.a"],
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
/// Planted `join1_no_j_lock` leaves the page running through (the wait below
/// never ends).
#[tokio::test]
async fn an_a_page_holds_its_j_row_until_it_commits() {
    let _serial = serial().await;
    for kind in KINDS {
        let mut d = start_kinds(&[kind], A_ROWS, B_ROWS).await;
        let b = BWriter::start(&d, kind, 10, 999).await;
        write(&d, "insert into public.a values (5, 10, 5, 'five')").await;
        let batch = d.seal().await;
        let drain = d.drain_frozen(batch, "page", &[]).await;
        d.wait_blocked_behind(b.pid).await;
        assert!(!drain.is_finished(), "the page queues on J[10]");
        b.commit().await;
        drain.finish().await;
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
        let drain = d.drain_frozen(batch, "page", &[]).await;
        d.wait_blocked_behind(b.pid).await;
        b.commit().await;
        drain.finish().await;
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
