//! A 1-1 `JOIN`'s chunked build, its rebuild sweep and its `RESUME`
//! (ADR-0021, #1400): deterministic interleavings of a build step against the
//! drain's pages, each family pinned by the plant that removes its rule.
//!
//! The two sides are keyed differently (`a` by `text`, `b` by `bigint`) and
//! name columns like the aliases the build's statements use (`r`, `s`,
//! `img`, `k0`), so a statement that reads a generated name bare, or keys
//! the target by the wrong side, fails here. Every test turns the dev knob
//! on before its driver starts, and ends with the oracle: the target equals
//! the live join, `R` equals `a` and `M` equals `b`, self_check clean.
//!
//! - `c…`: #1375's build-chunk families (C1–C10): a chunk against a page on
//!   one of its keys, and C10, a row or entry that moves after the chunk's
//!   unlocked read, one test for each of the chunk's three checks (its
//!   insert-from-read, its held keys, its Re-derive read).
//! - `h…`: #1481's `RESUME` families: H1, the rows of `a` rows deleted while
//!   paused (with the `tail` and `emptied` cases), and H3, a row re-inserted
//!   between the orphan pass's read and its re-derive. Also a `RESUME` after
//!   changes of both sides, and a sweep killed in its orphan pass.
//! - An `M` build with a rebuild sweep, and the build start under the lock
//!   row (#1400's new families), for one definition and for two whose lock
//!   rows a page takes in another order than their ids.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, Step, WorkerOptions};
use trellis::staging::interleave::PausePoint;
use trellis::{Trellis, TrellisOptions};

const DDL: &str = "create table public.a (id text primary key, k integer, r integer, s text); \
                   create table public.b (bid bigint primary key, k integer, img integer, \
                       k0 text); \
                   create unique index on public.b (k); \
                   create index on public.a (k)";

/// Twenty `a` rows on keys 0 to 60 (every fifth on `NULL`), six `b` rows on
/// keys 10 to 60: some `a` rows have no match.
const SEED: &str = "insert into public.a \
                        select 'a' || lpad(i::text, 2, '0'), \
                               case when i % 5 = 0 then null else (i % 7) * 10 end, \
                               i, 's' || i \
                        from generate_series(1, 20) i; \
                    insert into public.b select i, i * 10, i * 100, 'k' || i \
                        from generate_series(1, 6) i";

/// The forced key moves and the retry counters are process-wide, so these
/// tests run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Left,
    Inner,
}

const KINDS: [Kind; 2] = [Kind::Left, Kind::Inner];

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Left => "jl",
            Kind::Inner => "ji",
        }
    }

    fn target(self) -> String {
        format!("public.{}", self.name())
    }

    fn word(self) -> &'static str {
        match self {
            Kind::Left => "LEFT",
            Kind::Inner => "INNER",
        }
    }

    fn definition(self) -> String {
        format!(
            "TRANSFORM {} FROM public.a {} JOIN public.b ON a.k = b.k \
             SELECT a.r + COALESCE(b.img, 0) AS total, b.k0 AS k0, a.s AS s",
            self.name(),
            self.word()
        )
    }
}

fn columns() -> Vec<(&'static str, ValueType)> {
    vec![
        ("id", ValueType::Text),
        ("k", ValueType::Integer(IntWidth::Int4)),
        ("r", ValueType::Integer(IntWidth::Int4)),
        ("s", ValueType::Text),
    ]
}

/// `kinds` defined over the seeded tables, both captured; built and `live`
/// when `built`, or with their builds started and left for the test to run.
async fn start(kinds: &[Kind], built: bool) -> Driver {
    set_enabled(true);
    let ddl = format!("{DDL}; {SEED}");
    let definitions: Vec<String> = kinds.iter().map(|k| k.definition()).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let columns = columns();
    if built {
        Driver::start(&ddl, &columns, &definitions, &["public.a", "public.b"]).await
    } else {
        Driver::start_unbuilt(&[], &ddl, &columns, &definitions, &["public.a", "public.b"]).await
    }
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

/// Runs `sql` on `table` with its capture triggers disabled: a change no
/// entry hears about. (They are `ENABLE ALWAYS`.)
async fn write_uncaptured(d: &Driver, table: &str, sql: &str) {
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

async fn definition_id(d: &Driver, kind: Kind) -> i64 {
    d.ctl
        .query_one(
            "select id from transform_definitions where target_table = $1",
            &[&kind.target()],
        )
        .await
        .expect("the definition")
        .get(0)
}

/// Asserts `kind`'s target equals the live join, `R` holds `a`'s live rows
/// and `M` `b`'s. (`Driver::rows` reads its rows as `r`, so no checked
/// column is named `r`.)
async fn assert_kind(d: &Driver, kind: Kind, step: &str) {
    let actual = d
        .rows(&format!(
            "select id, total, k0, s from {} order by id",
            kind.target()
        ))
        .await;
    let expected = d
        .rows(&format!(
            "select a.id, a.r + coalesce(b.img, 0) as total, b.k0, a.s \
             from public.a a {} join public.b b on a.k = b.k order by a.id",
            kind.word()
        ))
        .await;
    assert_eq!(
        actual, expected,
        "{kind:?} target (left) differs from the live join (right) after {step}"
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &kind.target())
        .await
        .expect("self_check");
    assert!(diverging.is_empty(), "{kind:?} after {step}: {diverging:?}");
    let id = definition_id(d, kind).await;
    let held = d
        .rows(&format!(
            "select __from_key, k, r as c_r, s from {DEFAULT_SCHEMA}.join1_{id}_r \
             where not __tombstone order by 1"
        ))
        .await;
    let live = d
        .rows("select id, k, r as c_r, s from public.a order by 1")
        .await;
    assert_eq!(held, live, "{kind:?} R (left) differs from a after {step}");
    let held = d
        .rows(&format!(
            "select __from_key, k, img, k0 from {DEFAULT_SCHEMA}.join1_{id}_m \
             where not __tombstone order by 1"
        ))
        .await;
    let live = d
        .rows("select bid::text, k, img, k0 from public.b order by 1")
        .await;
    assert_eq!(held, live, "{kind:?} M (left) differs from b after {step}");
}

/// Settles the pipeline and every build, then asserts each of `kinds`.
async fn assert_oracle(d: &mut Driver, kinds: &[Kind], step: &str) {
    build::settle_builds_with(d.pool(), 3).await;
    d.settle().await;
    for kind in kinds {
        assert_kind(d, *kind, step).await;
    }
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
}

fn options(chunk_rows: i64) -> WorkerOptions {
    WorkerOptions {
        chunk_rows,
        drain_batch_cap: 10_000,
        heartbeat_interval: std::time::Duration::from_secs(1),
        reclaim_ttl: std::time::Duration::from_secs(60),
    }
}

/// One drain worker's build step, as `work_once` takes it.
async fn build_step(pool: &trellis::Pool, worker: &str, chunk_rows: i64) -> Step {
    build::work_once(pool, worker, &options(chunk_rows))
        .await
        .expect("a build step")
}

/// How many of the definitions' plan and chunk rows are still to run.
async fn pending(d: &Driver, kinds: &[&str]) -> i64 {
    let kinds: Vec<String> = kinds.iter().map(|k| k.to_string()).collect();
    d.ctl
        .query_one(
            "select count(*) from backfill_chunks where not done and kind = any($1)",
            &[&kinds],
        )
        .await
        .expect("count the build's jobs")
        .get(0)
}

/// Runs build steps until no plan job or chunk is left, so the next step
/// claims the sweep.
async fn run_until_the_sweep(d: &Driver) {
    for _ in 0..100 {
        if pending(d, &["plan", "rederive"]).await == 0 {
            return;
        }
        build_step(d.pool(), "builder", 3).await;
    }
    panic!("the plan and its chunks never finished");
}

async fn trellis(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

// --------------------------------------------------------- the chunked build

/// A fresh build of a 20-row `a` in chunks of 3: the plan job re-derives every
/// `b` row, then enqueues 7 chunks, and the target, `R` and `M` come out as
/// the live join. A fresh build over an empty target gets no sweep.
#[tokio::test]
async fn a_build_of_several_chunks_fills_the_target_r_and_m() {
    let _serial = SERIAL.lock().await;
    let mut d = start(&KINDS, false).await;
    assert_eq!(
        pending(&d, &["sweep"]).await,
        0,
        "an empty target needs none"
    );
    assert_eq!(build_step(d.pool(), "planner", 3).await, Step::Planned);
    // The first plan job walked `a` in ranges of 3 rows, after its `M` pass.
    assert_eq!(pending(&d, &["rederive"]).await, 7);
    let id = d
        .ctl
        .query_one(
            "select definition_id from backfill_chunks where kind = 'rederive' limit 1",
            &[],
        )
        .await
        .expect("a chunk")
        .get::<_, i64>(0);
    let m: i64 = d
        .ctl
        .query_one(
            &format!(
                "select count(*) from {DEFAULT_SCHEMA}.join1_{id}_m where __basis is not null"
            ),
            &[],
        )
        .await
        .expect("count M")
        .get(0);
    assert_eq!(m, 6, "M is built before any chunk");
    // Pages apply between the chunks.
    write(
        &d,
        "update public.a set r = 100 where id = 'a01'; \
               update public.b set img = 999 where bid = 2",
    )
    .await;
    let batch = d.seal().await;
    build_step(d.pool(), "builder", 3).await;
    d.drain(batch, "page").await;
    assert_oracle(&mut d, &KINDS, "the build").await;
}

/// A chunk frozen with everything written holds its `J` rows, so a `b` page
/// on one of its keys queues behind it, and reads `R` and the target only
/// once the chunk commits (#1375's C families: a chunk first).
#[tokio::test]
async fn c_a_page_on_a_chunks_key_queues_behind_the_chunk() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(&[kind], false).await;
        assert_eq!(build_step(d.pool(), "planner", 100).await, Step::Planned);
        let mut chunk = d
            .run_frozen(
                &[(PausePoint::BeforeRebuildCommit, &kind.target())],
                |pool| async move { Ok(build_step(&pool, "chunk", 100).await) },
            )
            .await;
        let reached = chunk.reached(PausePoint::BeforeRebuildCommit).await;
        write(&d, "update public.b set img = 777 where bid = 1").await;
        let batch = d.seal().await;
        let page = d.drain_frozen(batch, "page", &[]).await;
        d.wait_blocked_behind(reached.backend_pid).await;
        assert!(!page.is_finished(), "the page queued on the chunk's J row");
        d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
        chunk.finish().await;
        page.finish().await;
        assert_oracle(&mut d, &[kind], "a page behind a chunk").await;
    }
}

/// A page frozen at its commit holds its `J` row; a chunk over the page's row
/// waits for it at most its short lock timeout, gives up and retries, so it
/// never holds up the drain behind it (#1375's C families: a page first).
#[tokio::test]
async fn c_a_chunk_on_a_pages_key_gives_up_and_retries() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(&[kind], false).await;
        assert_eq!(build_step(d.pool(), "planner", 100).await, Step::Planned);
        write(&d, "update public.a set r = 55, k = 20 where id = 'a01'").await;
        let batch = d.seal().await;
        let mut page = d
            .drain_frozen(batch, "page", &[(PausePoint::BeforeCommit, &kind.target())])
            .await;
        page.reached(PausePoint::BeforeCommit).await;
        assert_eq!(build_step(d.pool(), "chunk", 100).await, Step::Chunk);
        let failed: Option<String> = d
            .ctl
            .query_one(
                "select last_error from backfill_chunks where kind = 'rederive'",
                &[],
            )
            .await
            .expect("the chunk")
            .get(0);
        assert!(
            failed.as_deref().is_some_and(|e| e.contains("lock")),
            "the chunk gave up on the page's lock: {failed:?}"
        );
        d.release(&mut page, PausePoint::BeforeCommit).await;
        page.finish().await;
        assert_oracle(&mut d, &[kind], "a chunk behind a page").await;
    }
}

/// C10 (`chunk_nokey`): row `a01` moves from key 10 onto key 70 after the
/// chunk's unlocked read of its range, so its insert-from-read finds key 70,
/// which it holds no `J` row for. It rolls back and its retry locks 70.
/// Planted `join1_chunk_no_key` writes the row's entry and target row on 70
/// unlocked; a `b` insert's page on 70 then commits around the uncommitted entry,
/// its fan-out doesn't see it, and the chunk's row keeps no match.
#[tokio::test]
async fn c10_a_chunk_whose_row_moved_onto_a_key_it_did_not_lock_retries() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(&[kind], false).await;
        // No `b` row on 70 yet: the build's `M` pass reads none.
        assert_eq!(build_step(d.pool(), "planner", 100).await, Step::Planned);
        // The `b` insert on 70, sealed before the chunk opens (the seal gate
        // refuses while the chunk's transaction is open); its page drains
        // while the chunk is frozen.
        write(&d, "insert into public.b values (7, 70, 700, 'k7')").await;
        let insert = d.seal().await;
        let target = kind.target();
        let moves = trellis::dev::interleave::join_key_moves();
        let mut chunk = d
            .run_frozen(
                &[
                    (PausePoint::BeforeJoin1Lock, &target),
                    (PausePoint::BeforeRebuildCommit, &target),
                ],
                |pool| async move { Ok(build_step(&pool, "chunk", 100).await) },
            )
            .await;
        chunk.reached(PausePoint::BeforeJoin1Lock).await;
        write(&d, "update public.a set k = 70 where id = 'a01'").await;
        d.release(&mut chunk, PausePoint::BeforeJoin1Lock).await;
        // Frozen with its rows written (planted), or rolled back.
        let frozen = chunk
            .reached_or_finished(PausePoint::BeforeRebuildCommit)
            .await;
        d.drain(insert, "b insert").await;
        d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
        chunk.finish_result().await.expect("the chunk's build step");
        if frozen.is_none() {
            assert!(
                trellis::dev::interleave::join_key_moves() > moves,
                "the chunk rolled back on the moved key"
            );
        }
        let moved = d.seal().await;
        d.drain(moved, "the move").await;
        assert_oracle(&mut d, &[kind], "C10").await;
    }
}

/// Waits until some backend queues on a lock `pid` holds (`true`), or until
/// `page` finishes without one having queued (`false`), so a planted run
/// fails on what it wrote rather than on a timeout.
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
            "no backend queued behind pid {pid}, and the page didn't finish, within 60 s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// Starts a rebuild in place of a live join and runs its plan job, so `R`
/// holds every row on the key it had and the chunks are enqueued.
async fn rebuild_planned(d: &mut Driver) {
    // No segment retires while a build runs, so the drained ones go first.
    d.retire().await;
    trellis(d)
        .await
        .request_backfill("public.a")
        .await
        .expect("request a rebuild");
    assert_eq!(build_step(d.pool(), "planner", 100).await, Step::Planned);
}

/// C10 on the chunk's Re-derive read: on a rebuild, `a01` has its `R` entry
/// on key 10, so the chunk re-derives it rather than inserting it. `a01`
/// moves onto 70 after the chunk's unlocked read; its entry still holds 10,
/// but the chunk's read of `a` finds 70, which it holds no `J` row for, so
/// it rolls back and its retry locks 70. Planted `join1_rd_no_key` moves the
/// entry onto 70 unlocked: the `b` insert's page on 70 fans out around it,
/// and the move's own Apply is refused (the chunk's read covers it), so the
/// row keeps no match.
#[tokio::test]
async fn c10_a_chunk_whose_rederived_row_moved_onto_a_key_it_did_not_lock_retries() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(&[kind], true).await;
        rebuild_planned(&mut d).await;
        // Sealed before the chunk opens (the seal gate refuses while the
        // chunk's transaction is open).
        write(&d, "insert into public.b values (7, 70, 700, 'k7')").await;
        let insert = d.seal().await;
        let target = kind.target();
        let moves = trellis::dev::interleave::join_key_moves();
        let mut chunk = d
            .run_frozen(
                &[
                    (PausePoint::BeforeJoin1Lock, &target),
                    (PausePoint::BeforeRebuildCommit, &target),
                ],
                |pool| async move { Ok(build_step(&pool, "chunk", 100).await) },
            )
            .await;
        chunk.reached(PausePoint::BeforeJoin1Lock).await;
        write(&d, "update public.a set k = 70 where id = 'a01'").await;
        d.release(&mut chunk, PausePoint::BeforeJoin1Lock).await;
        let frozen = chunk
            .reached_or_finished(PausePoint::BeforeRebuildCommit)
            .await;
        d.drain(insert, "b insert").await;
        d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
        chunk.finish_result().await.expect("the chunk's build step");
        if frozen.is_none() {
            assert!(
                trellis::dev::interleave::join_key_moves() > moves,
                "the chunk rolled back on the moved key"
            );
        }
        let moved = d.seal().await;
        d.drain(moved, "the move").await;
        assert_oracle(&mut d, &[kind], "C10 on the Re-derive read").await;
    }
}

/// C10 on the chunk's held keys: on a rebuild, `a01` moves onto 70 and back
/// onto 10, neither drained, so the chunk reads `a01` on 10 and its `R`
/// entry on 10, unlocked. While it waits before its `J` lock,
/// the first move's page moves the entry onto 70. The chunk's read of `a`
/// finds 10, in its key set, but the entry it locks holds 70, which it holds
/// no `J` row for, so it rolls back and its retry locks 70. Planted
/// `join1_no_recheck` moves the entry off 70 unlocked: the `b` insert's page
/// on 70 reads the entry still on 70 and joins `a01` to the new row, and the
/// move back's Apply is refused (the chunk's read covers it).
#[tokio::test]
async fn c10_a_chunk_whose_entry_moved_off_its_key_set_retries() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(&[kind], true).await;
        rebuild_planned(&mut d).await;
        // The build's segment, sealed and drained, so the moves below are
        // in later segments: their pages apply them rather than re-derive.
        step(&mut d, "update public.a set s = s where id = 'a20'").await;
        // Each sealed apart, so each drains alone; the move back is sealed
        // once the chunk is done (the seal gate refuses while the chunk is
        // open).
        write(&d, "update public.a set k = 70 where id = 'a01'").await;
        let forth = d.seal().await;
        write(&d, "insert into public.b values (7, 70, 700, 'k7')").await;
        let insert = d.seal().await;
        write(&d, "update public.a set k = 10 where id = 'a01'").await;
        let target = kind.target();
        let moves = trellis::dev::interleave::join_key_moves();
        let mut chunk = d
            .run_frozen(
                &[
                    (PausePoint::BeforeJoin1Lock, &target),
                    (PausePoint::BeforeRebuildCommit, &target),
                ],
                |pool| async move {
                    // Claimed by hand: with two batches sealed and not
                    // drained, a worker's build step wouldn't claim it.
                    let claimed = {
                        let client = pool.get().await.expect("pool");
                        trellis::defs::chunk_queue::claim_chunks_of(
                            &**client,
                            "chunk",
                            1,
                            &[trellis::defs::chunk_queue::KIND_REDERIVE],
                        )
                        .await
                        .expect("claim the chunk")
                    };
                    let chunk = claimed.into_iter().next().expect("a chunk");
                    build::run_claimed(&pool, &chunk, "chunk", &options(100)).await;
                    Ok(())
                },
            )
            .await;
        let reached = chunk.reached(PausePoint::BeforeJoin1Lock).await;
        // The chunk holds only the lock row `FOR SHARE` here, so the page
        // runs through.
        d.drain(forth, "the move onto 70").await;
        d.release(&mut chunk, PausePoint::BeforeJoin1Lock).await;
        let frozen = chunk
            .reached_or_finished(PausePoint::BeforeRebuildCommit)
            .await;
        // Planted, the `b` page's fan-out reaches `a01`'s target row, which
        // the frozen chunk holds.
        let page = d.drain_frozen(insert, "b insert", &[]).await;
        if frozen.is_some() {
            queued_or_finished(&d, reached.backend_pid, &page).await;
        }
        d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
        chunk.finish_result().await.expect("the chunk's build step");
        page.finish().await;
        if frozen.is_none() {
            assert!(
                trellis::dev::interleave::join_key_moves() > moves,
                "the chunk rolled back on the moved entry"
            );
        }
        let back = d.seal().await;
        d.drain(back, "the move back").await;
        assert_oracle(&mut d, &[kind], "C10 on the held keys").await;
    }
}

/// A row whose field fails on its data fails its chunk, which the chunk
/// queue narrows to that key (`fail_chunk`: split, then quarantine), as for
/// a plain 1-1 target. The build then goes on without the key, and every
/// other row is built.
#[tokio::test]
async fn a_row_whose_field_fails_is_quarantined_and_the_build_goes_on() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let d = start(&[kind], false).await;
        // `a02` is on key 20, whose `b` row adds 200: past `integer`.
        write_uncaptured(
            &d,
            "public.a",
            "update public.a set r = 2147483600 where id = 'a02'",
        )
        .await;
        build::settle_builds_with(d.pool(), 3).await;
        assert_eq!(
            d.rows(&format!(
                "select status from transform_definitions where target_table = '{}'",
                kind.target()
            ))
            .await,
            ["(live)"]
        );
        assert_eq!(
            d.rows("select src_table, key from poison").await,
            ["(public.a,a02)"]
        );
        let actual = d
            .rows(&format!(
                "select id, total, k0, s from {} where id <> 'a02' order by id",
                kind.target()
            ))
            .await;
        let expected = d
            .rows(&format!(
                "select a.id, a.r + coalesce(b.img, 0) as total, b.k0, a.s \
                 from public.a a {} join public.b b on a.k = b.k \
                 where a.id <> 'a02' order by a.id",
                kind.word()
            ))
            .await;
        assert_eq!(actual, expected, "{kind:?}: every other row is built");
    }
}

// ------------------------------------------------------ RESUME and the sweep

/// Pauses `kinds`, runs `sql` on `a` and drains it while they're paused (so
/// no page will ever apply it to them), then resumes them.
async fn change_while_paused(d: &mut Driver, kinds: &[Kind], sql: &str) {
    let trellis = trellis(d).await;
    for kind in kinds {
        trellis
            .apply(&format!("PAUSE TRANSFORM {}", kind.name()))
            .await
            .expect("pause");
    }
    step(d, sql).await;
    for kind in kinds {
        trellis
            .apply(&format!("RESUME TRANSFORM {}", kind.name()))
            .await
            .expect("resume");
    }
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the resumed builds");
}

/// H1: a `RESUME` recreates `R` empty and keeps the target, so the target
/// rows of `a` rows deleted while paused are backed by nothing. The sweep's
/// orphan pass, which a start over a non-empty target always enqueues,
/// re-derives them as gone. Rows deleted from the middle, the `tail` (rows
/// above every key the chunks reach) and `emptied` (all of `a`, no chunk at
/// all). Planted `join1_no_orphan_pass` leaves them in the target.
#[tokio::test]
async fn h1_a_resume_removes_the_rows_of_a_rows_deleted_while_paused() {
    let _serial = SERIAL.lock().await;
    for (case, sql) in [
        (
            "mixed",
            "delete from public.a where id in ('a02', 'a07', 'a11'); \
             update public.a set r = 1000 where id = 'a03'",
        ),
        ("tail", "delete from public.a where id > 'a17'"),
        ("emptied", "delete from public.a"),
    ] {
        let mut d = start(&KINDS, true).await;
        change_while_paused(&mut d, &KINDS, sql).await;
        assert!(
            pending(&d, &["sweep"]).await >= 2,
            "{case}: each start over a non-empty target enqueues the sweep"
        );
        assert_oracle(&mut d, &KINDS, case).await;
    }
}

/// H3: the orphan pass reads `a03`'s target row as an orphan (`a03` was
/// deleted while paused), then `a03` is inserted again and its page applies
/// before the pass re-derives it. The re-derive locks the entry the page
/// wrote and re-reads `a`, so the row stays. Planted `join1_orphan_delete`
/// deletes the orphans' rows by statement, and the re-inserted row is lost.
#[tokio::test]
async fn h3_a_row_reinserted_after_the_orphan_read_is_kept() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(&[kind], true).await;
        change_while_paused(&mut d, &[kind], "delete from public.a where id = 'a03'").await;
        run_until_the_sweep(&d).await;
        let target = kind.target();
        let mut sweep = d
            .run_frozen(
                &[(PausePoint::AfterJoin1OrphanRead, &target)],
                |pool| async move { Ok(build_step(&pool, "sweep", 3).await) },
            )
            .await;
        sweep.reached(PausePoint::AfterJoin1OrphanRead).await;
        assert_eq!(
            d.rows(&format!("select id from {target} where id = 'a03'"))
                .await,
            ["(a03)"],
            "the orphan is still in the target"
        );
        step(&mut d, "insert into public.a values ('a03', 30, 3, 's3')").await;
        d.release(&mut sweep, PausePoint::AfterJoin1OrphanRead)
            .await;
        sweep.finish().await;
        assert_oracle(&mut d, &[kind], "H3").await;
    }
}

/// An `M` build with a rebuild sweep: a rebuild in place of a live join
/// whose `b` row 2 (key 20) was deleted, and row 1 changed, past capture.
/// The build's `M` pass reads only the rows `b` has, so `M` keeps row 2's
/// live entry and the chunks join `a`'s rows on 20 to it. The sweep's stale
/// pass over `M` re-derives it as gone and fans out. `a` rows changed past
/// capture are the chunks' and `R`'s stale pass's. Planted
/// `join1_sweep_r_only` skips `M`'s pass, and the rows on 20 keep row 2.
#[tokio::test]
async fn the_sweep_tombstones_the_m_entries_of_b_rows_gone_before_a_rebuild() {
    let _serial = SERIAL.lock().await;
    let mut d = start(&KINDS, true).await;
    write_uncaptured(
        &d,
        "public.b",
        "delete from public.b where bid = 2; update public.b set img = 150 where bid = 1",
    )
    .await;
    write_uncaptured(
        &d,
        "public.a",
        "delete from public.a where id = 'a20'; update public.a set k = 40 where id = 'a01'",
    )
    .await;
    trellis(&d)
        .await
        .request_backfill("public.b")
        .await
        .expect("request a rebuild");
    assert_eq!(
        d.rows("select kind from backfill_chunks where not done order by kind, definition_id")
            .await,
        ["(plan)", "(plan)", "(sweep)", "(sweep)"],
        "a 1-1 JOIN's rebuild gets a plan job and a sweep"
    );
    assert_oracle(&mut d, &KINDS, "the rebuild").await;
}

/// The build start under the lock row: a rebuild start of a live join takes
/// the pair's lock row `FOR UPDATE` before it stamps the build's segment, so
/// it waits for a page in flight on the definition, which holds the row
/// `FOR SHARE` to its commit. (Whether the rule is needed is untested, as
/// in the #1375 model; this pins that the start queues.)
#[tokio::test]
async fn a_rebuild_start_waits_for_the_page_in_flight_on_its_lock_row() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(&[kind], true).await;
        write(&d, "update public.a set r = 77 where id = 'a04'").await;
        let batch = d.seal().await;
        let mut page = d
            .drain_frozen(batch, "page", &[(PausePoint::BeforeCommit, &kind.target())])
            .await;
        let reached = page.reached(PausePoint::BeforeCommit).await;
        let trellis = trellis(&d).await;
        let rebuild = tokio::spawn(async move { trellis.request_backfill("public.a").await });
        d.wait_blocked_behind(reached.backend_pid).await;
        assert!(!rebuild.is_finished(), "the start queued on the lock row");
        d.release(&mut page, PausePoint::BeforeCommit).await;
        page.finish().await;
        rebuild
            .await
            .expect("the rebuild task")
            .expect("request a rebuild");
        assert_oracle(&mut d, &[kind], "a rebuild behind a page").await;
    }
}

/// A rebuild of two 1-1 `JOIN`s over one table takes their lock rows in the
/// order a page takes them, target name order, not definition id order.
/// `jl` is defined first (the lower id) but `ji` sorts first: a page of an
/// `a` change frozen with `ji`'s lock row held `FOR SHARE` makes the rebuild
/// queue on that row before it holds `jl`'s, so the page then takes `jl`'s
/// and commits. Taken in id order, the rebuild holds `jl`'s `FOR UPDATE`
/// while it queues on `ji`'s, the page queues on `jl`'s, and the server
/// breaks the deadlock by failing one of them.
#[tokio::test]
async fn a_rebuild_of_two_joins_takes_their_lock_rows_in_a_pages_order() {
    let _serial = SERIAL.lock().await;
    let mut d = start(&KINDS, true).await;
    write(&d, "update public.a set r = 77 where id = 'a04'").await;
    let batch = d.seal().await;
    let inner = Kind::Inner.target();
    let mut page = d
        .drain_frozen(batch, "page", &[(PausePoint::BeforeJoin1Lock, &inner)])
        .await;
    let reached = page.reached(PausePoint::BeforeJoin1Lock).await;
    let trellis = trellis(&d).await;
    let rebuild = tokio::spawn(async move { trellis.request_backfill("public.a").await });
    d.wait_blocked_behind(reached.backend_pid).await;
    d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
    page.finish_result().await.expect("the page");
    rebuild
        .await
        .expect("the rebuild task")
        .expect("request a rebuild");
    assert_oracle(&mut d, &KINDS, "a rebuild of two joins behind a page").await;
}

/// A `RESUME` after changes of every kind while paused, on both sides: an
/// `a` row inserted, one inserted on a `NULL` key, one moved onto a `NULL`
/// key (its `INNER` row must go, its `LEFT` row lose its match), one moved
/// onto another key, one deleted; a `b` row deleted, one changed and one
/// inserted on a key some `a` rows have. `R` and `M` come back empty, so the
/// chunks rebuild every row from the `M` the plan job made, and the sweep's
/// orphan pass removes the deleted row's.
#[tokio::test]
async fn a_resume_after_changes_of_both_sides_while_paused_rebuilds_the_join() {
    let _serial = SERIAL.lock().await;
    let mut d = start(&KINDS, true).await;
    change_while_paused(
        &mut d,
        &KINDS,
        "insert into public.a values ('a21', 20, 21, 's21'), ('a22', null, 22, 's22'); \
         update public.a set k = null where id = 'a01'; \
         update public.a set k = 70 where id = 'a03'; \
         delete from public.a where id = 'a04'; \
         delete from public.b where bid = 2; \
         update public.b set img = 150 where bid = 1; \
         insert into public.b values (7, 70, 700, 'k7')",
    )
    .await;
    assert_oracle(&mut d, &KINDS, "a resume after changes of both sides").await;
}

/// A sweep killed in its orphan pass, after `R`'s and `M`'s stale passes
/// committed their cursors, is claimed again and resumes from its stored
/// cursor: every orphan of the `a` rows deleted while paused goes, and no
/// other row does.
#[tokio::test]
async fn a_sweep_killed_in_its_orphan_pass_resumes_from_its_cursor() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start(&[kind], true).await;
        change_while_paused(
            &mut d,
            &[kind],
            "delete from public.a where id in ('a02', 'a05', 'a08', 'a11', 'a14', 'a17')",
        )
        .await;
        run_until_the_sweep(&d).await;
        let target = kind.target();
        // Its own pool, so the session ended below is no connection the
        // test's pool hands out again.
        let own = trellis::Pool::new(&Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"))
            .expect("a pool for the sweep");
        let mut sweep = d
            .run_frozen(
                &[(PausePoint::AfterJoin1OrphanRead, &target)],
                |_| async move { Ok(build_step(&own, "sweep", 3).await) },
            )
            .await;
        let reached = sweep.reached(PausePoint::AfterJoin1OrphanRead).await;
        let stored: Option<String> = d
            .ctl
            .query_one(
                "select lo from backfill_chunks where kind = 'sweep' and not done",
                &[],
            )
            .await
            .expect("the sweep")
            .get(0);
        assert_eq!(
            stored.as_deref(),
            Some("\u{1}o"),
            "the stale passes are done and committed"
        );
        sweep.kill().await;
        d.ctl
            .execute("select pg_terminate_backend($1)", &[&reached.backend_pid])
            .await
            .expect("end the sweep's session");
        d.ctl
            .batch_execute(
                "update backfill_chunks \
                 set claimed_by = null, claimed_at = null, next_attempt_at = null \
                 where kind = 'sweep'",
            )
            .await
            .expect("free the dead worker's claim");
        assert_oracle(&mut d, &[kind], "a killed sweep").await;
    }
}
