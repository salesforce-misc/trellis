//! Deterministic interleavings of a 1-1 target's Re-derive build chunk
//! (#625 F8a; `trellis::staging::build::one_to_one`) with Apply pages, on
//! the real engine, driven by hand through `support/drain_driver.rs`. No
//! test sleeps or polls for convergence (#297).
//!
//! Each test makes the state a build starts from by hand, as
//! `build_interleavings.rs` does for an aggregate: the definition is
//! installed and applying, and then its ledger and target are emptied. The
//! source rows are then in the target only once a chunk or an Apply writes
//! them. Every test ends by checking the target against the source.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::{Driver, Running};
use trellis::defs::ValueType;
use trellis::staging::build::one_to_one::{self, OneToOneOutcome, OneToOnePlan};
use trellis::staging::interleave::PausePoint;
use trellis::staging::quarantine::{FailureClass, classify};

const TARGET: &str = "public.one";
const ONE: &str = "TRANSFORM one FROM public.src SELECT g AS g, v + v AS dbl";
const ACTUAL: &str = "select id, g, dbl from public.one order by id";
const EXPECTED: &str = "select id, g, v + v from public.src order by id";

const CREATE: &str = "create table public.src (id integer primary key, g integer, v numeric);";

fn columns() -> Vec<(&'static str, ValueType)> {
    vec![
        ("id", ValueType::Numeric),
        ("g", ValueType::Numeric),
        ("v", ValueType::Numeric),
    ]
}

fn seed(rows: &[(i32, i32, i32)]) -> String {
    let values: Vec<String> = rows
        .iter()
        .map(|(id, g, v)| format!("({id}, {g}, {v})"))
        .collect();
    format!("insert into public.src values {};", values.join(", "))
}

/// `public.src (id, g, v)` seeded with `rows`, `definitions` live over it
/// (the first being [`ONE`]), and then the 1-1 target and its ledger
/// emptied: where a build starts. Returns the driver and the 1-1 plan.
async fn start_build_with(
    rows: &[(i32, i32, i32)],
    definitions: &[&str],
) -> (Driver, OneToOnePlan) {
    let d = Driver::start(
        &format!("{CREATE} {}", seed(rows)),
        &columns(),
        definitions,
        &["public.src"],
    )
    .await;
    d.ctl
        .batch_execute("truncate public.one__ledger, public.one")
        .await
        .expect("empty what the build writes");
    let plan = OneToOnePlan::load(d.pool(), "one")
        .await
        .expect("load the 1-1 plan")
        .expect("a plain 1-1 target takes the re-derive build");
    (d, plan)
}

async fn start_build(rows: &[(i32, i32, i32)]) -> (Driver, OneToOnePlan) {
    start_build_with(rows, &[ONE]).await
}

/// Starts one 1-1 chunk, `(lo, hi]`, in its own task and transaction,
/// frozen at each of `points` once reached.
async fn chunk_frozen(
    d: &Driver,
    plan: &OneToOnePlan,
    lo: Option<&str>,
    hi: &str,
    points: &[(PausePoint, &str)],
) -> Running<OneToOneOutcome> {
    let plan = plan.clone();
    let (lo, hi) = (lo.map(str::to_string), hi.to_string());
    d.run_frozen(points, move |pool| async move {
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        let outcome = one_to_one::run_chunk(&txn, &plan, lo.as_deref(), &hi).await?;
        txn.commit().await?;
        Ok(outcome)
    })
    .await
}

/// Runs and commits one 1-1 chunk.
async fn chunk(d: &Driver, plan: &OneToOnePlan, lo: Option<&str>, hi: &str) -> OneToOneOutcome {
    chunk_frozen(d, plan, lo, hi, &[]).await.finish().await
}

/// Settles the pipeline and asserts the target equals the source.
async fn assert_oracle(d: &mut Driver) {
    d.settle().await;
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        "the 1-1 target against the source"
    );
}

// ------------------------------------------------- a chunk against an Apply

#[derive(Clone, Copy, Debug)]
enum Op {
    Insert,
    Update,
    Delete,
}

impl Op {
    fn sql(self) -> &'static str {
        match self {
            Op::Insert => "insert into public.src values (7, 1, 70)",
            Op::Update => "update public.src set v = v + 10, g = 2 where id = 2",
            Op::Delete => "delete from public.src where id = 2",
        }
    }
}

/// When the chunk runs against the write and its Apply.
#[derive(Clone, Copy, Debug)]
enum Order {
    /// The chunk, then the write, then its Apply: the Apply changes the
    /// entry the chunk wrote.
    ChunkWriteApply,
    /// The write, then the chunk, then the Apply: the chunk's snapshot saw
    /// the write, so I2's visibility clause refuses the Apply.
    WriteChunkApply,
    /// The write and its Apply, then the chunk: the Apply wrote the key from
    /// no entry, and the chunk re-derives it to the same row.
    WriteApplyChunk,
}

/// A chunk over keys 1–10 and an Apply of `op` (on key 2, or key 7 for the
/// insert), in every order (D1's 1-1 interleavings, against a chunk).
async fn chunk_against_apply(op: Op) {
    for order in [
        Order::ChunkWriteApply,
        Order::WriteChunkApply,
        Order::WriteApplyChunk,
    ] {
        let (mut d, plan) = start_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3), (4, 2, 4)]).await;
        let user = d.user().await;
        if matches!(order, Order::ChunkWriteApply) {
            assert_eq!(chunk(&d, &plan, None, "10").await.keys, 4);
        }
        user.batch_execute(op.sql()).await.expect("the write");
        if matches!(order, Order::WriteChunkApply) {
            chunk(&d, &plan, None, "10").await;
        }
        let batch = d.seal().await;
        d.drain(batch, "apply").await;
        if matches!(order, Order::WriteApplyChunk) {
            chunk(&d, &plan, None, "10").await;
        }
        assert_eq!(
            d.rows(ACTUAL).await,
            d.rows(EXPECTED).await,
            "{op:?} {order:?}"
        );
        assert_oracle(&mut d).await;
    }
}

#[tokio::test]
async fn a_chunk_and_an_apply_agree_on_an_insert() {
    chunk_against_apply(Op::Insert).await;
}

#[tokio::test]
async fn a_chunk_and_an_apply_agree_on_an_update() {
    chunk_against_apply(Op::Update).await;
}

#[tokio::test]
async fn a_chunk_and_an_apply_agree_on_a_delete() {
    chunk_against_apply(Op::Delete).await;
}

/// Two writes to one key commit before the chunk reads, and their Applies
/// come after it, the older first. The chunk's entry has no `applied_lsn`,
/// so only I2's visibility clause refuses the older Apply: without it, the
/// older write's image would put the row back behind the chunk's until the
/// newer one drained. Each `(first, second)` is a write and a later one to
/// the same key.
#[tokio::test]
async fn an_older_apply_the_chunks_snapshot_saw_is_refused() {
    let pairs = [
        (Op::Insert, "update public.src set v = 71 where id = 7"),
        (
            Op::Update,
            "update public.src set v = 22, g = 3 where id = 2",
        ),
        (Op::Delete, "insert into public.src values (2, 3, 30)"),
    ];
    for (first, second) in pairs {
        let (mut d, plan) = start_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
        let user = d.user().await;
        user.batch_execute(first.sql())
            .await
            .expect("the first write");
        let older = d.seal().await;
        user.batch_execute(second).await.expect("the second write");
        let newer = d.seal().await;
        chunk(&d, &plan, None, "10").await;
        d.drain(older, "older").await;
        assert_eq!(
            d.rows(ACTUAL).await,
            d.rows(EXPECTED).await,
            "{first:?}: the older Apply doesn't move the row off the chunk's"
        );
        d.drain(newer, "newer").await;
        assert_oracle(&mut d).await;
    }
}

/// Runs `sql` on the source with its capture triggers disabled, in one
/// transaction: a change no ledger entry will ever hear about.
async fn write_uncaptured(d: &Driver, sql: &str) {
    let toggle = |action: &str| {
        format!(
            "do $$ declare t text; begin \
                 for t in select tgname from pg_trigger \
                          where tgrelid = 'public.src'::regclass and not tgisinternal loop \
                     execute format('alter table public.src {action} trigger %I', t); \
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

// ------------------------------------------------------------ idempotence

/// A chunk run again finds every row equal to its source and writes
/// nothing; a key deleted under it (between its key read and its lock)
/// becomes a tombstone and its row goes.
#[tokio::test]
async fn a_chunk_run_again_writes_nothing() {
    let (mut d, plan) = start_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    let first = chunk(&d, &plan, None, "3").await;
    assert_eq!(
        first,
        OneToOneOutcome {
            keys: 3,
            written: 3,
            deleted: 0
        }
    );
    let again = chunk(&d, &plan, None, "3").await;
    assert_eq!(
        again,
        OneToOneOutcome {
            keys: 3,
            written: 0,
            deleted: 0
        },
        "a re-run writes no row"
    );
    assert_oracle(&mut d).await;
}

/// A key the chunk locked whose row is deleted before its read becomes a
/// tombstone, and its target row (written by an earlier Apply) goes.
#[tokio::test]
async fn a_key_deleted_under_the_chunk_is_deleted_from_the_target() {
    let (mut d, plan) = start_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    let user = d.user().await;
    user.batch_execute("update public.src set v = 33 where id = 3")
        .await
        .expect("update key 3");
    let batch = d.seal().await;
    d.drain(batch, "apply").await;
    assert_eq!(d.rows(ACTUAL).await, vec!["(3,2,66)".to_string()]);
    let mut running = chunk_frozen(
        &d,
        &plan,
        None,
        "3",
        &[(PausePoint::AfterPlaceholders, TARGET)],
    )
    .await;
    running.reached(PausePoint::AfterPlaceholders).await;
    user.batch_execute("delete from public.src where id = 3")
        .await
        .expect("delete key 3 under the chunk");
    d.release(&mut running, PausePoint::AfterPlaceholders).await;
    let outcome = running.finish().await;
    assert_eq!(
        outcome,
        OneToOneOutcome {
            keys: 3,
            written: 2,
            deleted: 1
        }
    );
    let tombstone: bool = d
        .ctl
        .query_one(
            "select __tombstone from public.one__ledger where __from_key = '3'",
            &[],
        )
        .await
        .expect("read key 3's entry")
        .get(0);
    assert!(tombstone, "key 3's entry is a tombstone");
    assert_oracle(&mut d).await;
}

/// A quarantined key (`poison`) is left out of the chunk: no entry, no row.
#[tokio::test]
async fn a_chunk_leaves_a_quarantined_key_out() {
    let (d, plan) = start_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    d.ctl
        .batch_execute(
            "insert into poison (src_table, key, last_error) values ('public.src', '2', 'test')",
        )
        .await
        .expect("quarantine key 2");
    let outcome = chunk(&d, &plan, None, "3").await;
    assert_eq!(outcome.keys, 2);
    assert_eq!(
        d.rows(ACTUAL).await,
        vec!["(1,1,2)".to_string(), "(3,2,6)".to_string()]
    );
    let entries: i64 = d
        .ctl
        .query_one(
            "select count(*) from public.one__ledger where __from_key = '2'",
            &[],
        )
        .await
        .expect("count key 2's entries")
        .get(0);
    assert_eq!(
        entries, 0,
        "the chunk took no entry for the quarantined key"
    );
}

/// The chunk statement reaches the ledger, the target and the source each
/// through its key's index, whatever their statistics say (#625 F8a). At
/// 10M source rows the planner estimated the locked keys joined to their
/// rows at millions, and the entry rewrite hashed the whole ledger, so each
/// chunk cost grew with the ledger. Here the tables are tiny, where a
/// sequential scan is the plan the statistics pick.
#[tokio::test]
async fn the_chunk_statement_reads_every_table_by_its_key() {
    let rows: Vec<(i32, i32, i32)> = (1..=50).map(|i| (i, i % 5, i)).collect();
    let (mut d, plan) = start_build(&rows).await;
    chunk(&d, &plan, None, "50").await;
    d.ctl
        .batch_execute("analyze public.src, public.one, public.one__ledger")
        .await
        .expect("analyze the tables");
    let keys: Vec<String> = (11..=20).map(|i| i.to_string()).collect();
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let mut client = d.pool().get().await.expect("a connection");
    let txn = client.transaction().await.expect("begin");
    let explained = one_to_one::explain_chunk(&txn, &plan, Some("10"), "20", &key_refs)
        .await
        .expect("explain the chunk statement");
    txn.rollback().await.expect("roll back");
    drop(client);
    let scans: Vec<&str> = explained
        .lines()
        .filter(|line| line.contains("Seq Scan"))
        .collect();
    assert_eq!(
        scans,
        Vec::<&str>::new(),
        "no sequential scan:\n{explained}"
    );
    assert!(
        explained.contains("one__ledger_pkey"),
        "the entry rewrite reads the ledger's key index:\n{explained}"
    );
    assert_oracle(&mut d).await;
}

/// The chunk statement's plan setting (no sequential scans) ends with the
/// statement: whatever else the chunk's or a sweep batch's transaction runs
/// next (marking the chunk done, the sweep's cursor) plans as usual.
#[tokio::test]
async fn the_chunk_plan_setting_ends_with_its_statement() {
    let (d, plan) = start_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    let mut client = d.pool().get().await.expect("a connection");
    let seqscan = async |txn: &tokio_postgres::Transaction<'_>| -> String {
        txn.query_one("select current_setting('enable_seqscan')", &[])
            .await
            .expect("read enable_seqscan")
            .get(0)
    };

    let txn = client.transaction().await.expect("begin");
    let outcome = one_to_one::run_chunk(&txn, &plan, None, "3")
        .await
        .expect("run the chunk");
    assert_eq!(outcome.written, 3, "the chunk ran its statement");
    assert_eq!(seqscan(&txn).await, "on", "after a chunk");
    txn.commit().await.expect("commit the chunk");

    // Every entry's basis is older than a start past every xid, so the
    // sweep re-derives them all.
    let txn = client.transaction().await.expect("begin");
    let swept = one_to_one::sweep_batch(&txn, &plan, "18446744073709551615", None, 10)
        .await
        .expect("run a sweep batch");
    assert_eq!(swept.rederived, 3, "the sweep ran its statement");
    assert_eq!(seqscan(&txn).await, "on", "after a sweep batch");
    txn.commit().await.expect("commit the sweep batch");
}

// ------------------------------------------------------- chunk lock waits

/// A chunk frozen after its entry lock holds an existing key's entry (the
/// `chunk_without_entry_lock` plant fails here), so a page on that key
/// queues, and proceeds once the chunk commits.
#[tokio::test]
async fn a_page_queues_behind_a_chunk_holding_its_key() {
    let (mut d, plan) = start_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    // Key 2 gets an entry first, so the chunk locks it `for update` rather
    // than inserting it.
    let user = d.user().await;
    user.batch_execute("update public.src set v = 20 where id = 2")
        .await
        .expect("update key 2");
    let batch = d.seal().await;
    d.drain(batch, "apply").await;
    let mut running = chunk_frozen(
        &d,
        &plan,
        None,
        "3",
        &[(PausePoint::AfterEntryLock, TARGET)],
    )
    .await;
    let frozen = running.reached(PausePoint::AfterEntryLock).await;
    let probe = d
        .ctl
        .execute(
            "select 1 from public.one__ledger where __from_key = '2' for update nowait",
            &[],
        )
        .await
        .expect_err("the chunk holds key 2's entry");
    assert_eq!(
        probe.code(),
        Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE),
        "{probe}"
    );
    user.batch_execute("update public.src set v = v + 10 where id = 2")
        .await
        .expect("update key 2 again");
    let batch = d.seal().await;
    let page = d.drain_frozen(batch, "page", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut running, PausePoint::AfterEntryLock).await;
    assert_eq!(running.finish().await.keys, 3);
    page.finish().await;
    assert_oracle(&mut d).await;
}

/// A page frozen after its entry lock holds key 2, so a chunk over key 2
/// gives up at its short lock timeout with `55P03` (transient), writing
/// nothing. Once the page commits, the chunk runs again.
#[tokio::test]
async fn a_chunk_gives_up_on_a_key_a_page_holds() {
    let (mut d, plan) = start_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    let user = d.user().await;
    user.batch_execute("update public.src set v = v + 10 where id = 2")
        .await
        .expect("update key 2");
    let batch = d.seal().await;
    let mut page = d
        .drain_frozen(batch, "page", &[(PausePoint::AfterEntryLock, TARGET)])
        .await;
    page.reached(PausePoint::AfterEntryLock).await;
    let err = chunk_frozen(&d, &plan, None, "3", &[])
        .await
        .finish_result()
        .await
        .expect_err("the chunk gives up on key 2");
    assert!(
        trellis::locks::is_lock_not_available(&err),
        "a lock timeout, not {err}"
    );
    assert_eq!(classify(&err), FailureClass::Transient);
    assert!(d.rows(ACTUAL).await.is_empty(), "the chunk rolled back");
    d.release(&mut page, PausePoint::AfterEntryLock).await;
    page.finish().await;
    assert_eq!(chunk(&d, &plan, None, "3").await.keys, 3);
    assert_oracle(&mut d).await;
}

/// A chunk takes its entry lock before it reads (I1 for the build). Key 2
/// has an entry, and a page applying a later change to it is frozen after
/// its entry lock, before it writes. The chunk over key 2 queues behind it,
/// and its read sees the page's commit, so the row ends at the page's
/// change. The `chunk_without_entry_lock` plant doesn't fail this one: the
/// page's change was committed before the chunk's read, so the chunk writes
/// the same row either way (its entry rewrite still queues behind the
/// page). `a_page_queues_behind_a_chunk_holding_its_key` catches the plant.
#[tokio::test]
async fn a_chunk_reads_a_row_only_once_the_page_holding_it_commits() {
    let (mut d, plan) = start_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    let user = d.user().await;
    user.batch_execute("update public.src set v = v + 10 where id = 2")
        .await
        .expect("update key 2");
    let batch = d.seal().await;
    d.drain(batch, "apply").await;
    user.batch_execute("update public.src set v = v + 100 where id = 2")
        .await
        .expect("update key 2 again");
    let batch = d.seal().await;
    let mut page = d
        .drain_frozen(batch, "page", &[(PausePoint::AfterEntryLock, TARGET)])
        .await;
    let frozen = page.reached(PausePoint::AfterEntryLock).await;
    let running = chunk_frozen(&d, &plan, None, "3", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut page, PausePoint::AfterEntryLock).await;
    page.finish().await;
    match running.finish_result().await {
        Ok(outcome) => assert_eq!(outcome.keys, 3),
        Err(err) if trellis::locks::is_lock_not_available(&err) => {
            assert_eq!(chunk(&d, &plan, None, "3").await.keys, 3);
        }
        Err(err) => panic!("the chunk failed: {err}"),
    }
    assert_oracle(&mut d).await;
}

// ---------------------------------------------------------------- the seam

/// A chunk's writes reach a definition reading the 1-1 target through the
/// target-mutation seam, prior images included: a row the chunk moves
/// between groups leaves its old group.
#[tokio::test]
async fn a_chunk_feeds_a_reader_of_its_target_through_the_seam() {
    const READER_ACTUAL: &str = "select g, n, s from public.byg order by g";
    const READER_EXPECTED: &str =
        "select g, count(*), sum(dbl) from public.one group by g order by g";
    // Integer columns: a numeric group key can't key a chained reader.
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    let mut d = Driver::start(
        &format!("{CREATE} {}", seed(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)])),
        &[("id", int4), ("g", int4), ("v", ValueType::Numeric)],
        &[ONE],
        &["public.src"],
    )
    .await;
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM byg FROM public.one GROUP BY g SELECT COUNT(*) AS n, SUM(dbl) AS s",
        &std::collections::HashMap::from([
            ("id".to_string(), int4),
            ("g".to_string(), int4),
            ("dbl".to_string(), ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install the chained reader");
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    let plan = OneToOnePlan::load(d.pool(), "one")
        .await
        .expect("load the 1-1 plan")
        .expect("a 1-1 plan");
    // Changes no entry hears about: the target is stale, as a resumed
    // definition's is.
    write_uncaptured(
        &d,
        "update public.src set g = 3 where id = 1; update public.src set v = 50 where id = 3",
    )
    .await;
    let stale = d.rows(READER_ACTUAL).await;
    let outcome = chunk(&d, &plan, None, "3").await;
    assert_eq!(outcome.written, 2);
    d.settle().await;
    assert_ne!(d.rows(READER_ACTUAL).await, stale);
    assert_eq!(
        d.rows(READER_ACTUAL).await,
        d.rows(READER_EXPECTED).await,
        "the reader follows the chunk's writes, group moves included"
    );
    assert_oracle(&mut d).await;
}

// ------------------------------------------- a field build's chunk (F8b)
//
// `ALTER TRANSFORM one ADD v + 1 AS w` registers a field build (#625 F8b):
// `w` applies from the edit's commit, and each chunk of the build rewrites
// just `w` of its keys' existing rows from one snapshot, under their entry
// lock, leaving the entries alone. Each test makes that state from a live
// `one` and runs the build's chunks by hand.

const W_ACTUAL: &str = "select id, g, dbl, w from public.one order by id";
const W_EXPECTED: &str = "select id, g, v + v, v + 1 from public.src order by id";

/// `public.src` seeded with `rows`, [`ONE`] live over it, and then
/// `ALTER TRANSFORM one ADD v + 1 AS w`, with none of its field build run.
async fn start_field_build(rows: &[(i32, i32, i32)]) -> (Driver, OneToOnePlan) {
    let d = Driver::start(
        &format!("{CREATE} {}", seed(rows)),
        &columns(),
        &[ONE],
        &["public.src"],
    )
    .await;
    let trellis::defs::Statement::AlterTransform(alter) =
        trellis::defs::parse_statement("ALTER TRANSFORM one ADD v + 1 AS w").expect("parse")
    else {
        panic!("not an ALTER");
    };
    trellis::defs::alter_transform(d.pool(), &alter)
        .await
        .expect("alter one");
    let plan = OneToOnePlan::load(d.pool(), "one")
        .await
        .expect("load the 1-1 plan")
        .expect("a plain 1-1 target takes the re-derive build");
    (d, plan)
}

/// Starts one field chunk of `w`, `(lo, hi]`, in its own task and
/// transaction, frozen at each of `points` once reached.
async fn field_chunk_frozen(
    d: &Driver,
    plan: &OneToOnePlan,
    lo: Option<&str>,
    hi: &str,
    points: &[(PausePoint, &str)],
) -> Running<OneToOneOutcome> {
    let plan = plan.clone();
    let (lo, hi) = (lo.map(str::to_string), hi.to_string());
    d.run_frozen(points, move |pool| async move {
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        let outcome =
            one_to_one::run_field_chunk(&txn, &plan, &["w".to_string()], lo.as_deref(), &hi)
                .await?;
        txn.commit().await?;
        Ok(outcome)
    })
    .await
}

/// Runs and commits one field chunk of `w`.
async fn field_chunk(
    d: &Driver,
    plan: &OneToOnePlan,
    lo: Option<&str>,
    hi: &str,
) -> OneToOneOutcome {
    field_chunk_frozen(d, plan, lo, hi, &[])
        .await
        .finish()
        .await
}

/// Settles the pipeline and asserts the target, `w` included, equals the
/// source.
async fn assert_field_oracle(d: &mut Driver) {
    d.settle().await;
    assert_eq!(
        d.rows(W_ACTUAL).await,
        d.rows(W_EXPECTED).await,
        "the 1-1 target, its built field included, against the source"
    );
}

/// The edit itself writes no row, and a field chunk writes only `w`.
#[tokio::test]
async fn a_field_chunk_writes_only_its_field() {
    let (mut d, plan) = start_field_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    assert_eq!(
        d.rows(W_ACTUAL).await,
        ["(1,1,2,)", "(2,1,4,)", "(3,2,6,)"],
        "the edit only registers its build"
    );
    // A change no entry hears about: a field chunk doesn't bring `g` or
    // `dbl` up to date, only a whole build or Apply does.
    write_uncaptured(&d, "update public.src set g = 9, v = 5 where id = 1").await;
    const ENTRIES: &str = "select __from_key, __basis::text, __tombstone, __applied_seg \
                           from public.one__ledger order by __from_key";
    let entries = d.rows(ENTRIES).await;
    let outcome = field_chunk(&d, &plan, None, "3").await;
    assert_eq!(
        outcome,
        OneToOneOutcome {
            keys: 3,
            written: 3,
            deleted: 0,
        }
    );
    assert_eq!(
        d.rows(W_ACTUAL).await,
        ["(1,1,2,6)", "(2,1,4,3)", "(3,2,6,4)"],
        "w from the source as the chunk read it; g and dbl untouched"
    );
    // Run again, it finds `w` right everywhere and writes nothing.
    assert_eq!(field_chunk(&d, &plan, None, "3").await.written, 0);
    assert_eq!(
        d.rows(ENTRIES).await,
        entries,
        "a field chunk leaves every entry as it was"
    );
    // Put the source back as the target's `g` and `dbl` have it, and the
    // field chunk brings `w` along.
    write_uncaptured(&d, "update public.src set g = 1, v = 1 where id = 1").await;
    field_chunk(&d, &plan, None, "3").await;
    assert_field_oracle(&mut d).await;
}

/// A field chunk over keys 1–10 and an Apply of `op` (on key 2, or key 7 for
/// the insert), in every order: the field applies from the edit's commit, so
/// Apply writes `w` from its image whichever comes first.
async fn field_chunk_against_apply(op: Op) {
    for order in [
        Order::ChunkWriteApply,
        Order::WriteChunkApply,
        Order::WriteApplyChunk,
    ] {
        let (mut d, plan) = start_field_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3), (4, 2, 4)]).await;
        let user = d.user().await;
        if matches!(order, Order::ChunkWriteApply) {
            assert_eq!(field_chunk(&d, &plan, None, "10").await.keys, 4);
        }
        user.batch_execute(op.sql()).await.expect("the write");
        if matches!(order, Order::WriteChunkApply) {
            field_chunk(&d, &plan, None, "10").await;
        }
        let batch = d.seal().await;
        d.drain(batch, "apply").await;
        if matches!(order, Order::WriteApplyChunk) {
            field_chunk(&d, &plan, None, "10").await;
        }
        assert_eq!(
            d.rows(W_ACTUAL).await,
            d.rows(W_EXPECTED).await,
            "{op:?} {order:?}"
        );
        assert_field_oracle(&mut d).await;
    }
}

#[tokio::test]
async fn a_field_chunk_and_an_apply_agree_on_an_insert() {
    field_chunk_against_apply(Op::Insert).await;
}

#[tokio::test]
async fn a_field_chunk_and_an_apply_agree_on_an_update() {
    field_chunk_against_apply(Op::Update).await;
}

#[tokio::test]
async fn a_field_chunk_and_an_apply_agree_on_a_delete() {
    field_chunk_against_apply(Op::Delete).await;
}

/// Why a field chunk leaves the entries alone: a write to key 2 commits,
/// the chunk's snapshot sees it, and only then does the write's Apply run.
/// Had the chunk moved key 2's `basis` to its snapshot, ADR-0002's I2 would
/// refuse that Apply as already seen, and `g` and `dbl`, which the chunk
/// doesn't write, would keep their old values for good.
#[tokio::test]
async fn a_change_the_field_chunk_saw_still_applies_its_other_columns() {
    let (mut d, plan) = start_field_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    let user = d.user().await;
    user.batch_execute("update public.src set g = 5, v = 20 where id = 2")
        .await
        .expect("update key 2");
    field_chunk(&d, &plan, None, "3").await;
    assert_eq!(
        d.rows("select id, g, w from public.one where id = 2").await,
        ["(2,1,21)"],
        "the chunk wrote w from the change it saw, and nothing else"
    );
    let batch = d.seal().await;
    d.drain(batch, "apply").await;
    assert_eq!(
        d.rows("select id, g, dbl, w from public.one where id = 2")
            .await,
        ["(2,5,40,21)"],
        "the change's Apply still writes its other columns"
    );
    assert_field_oracle(&mut d).await;
}

/// A field chunk frozen after its entry lock holds key 1's entry (the
/// `chunk_without_entry_lock` plant fails here), so a page applying a later
/// change to key 1 queues behind it and writes `w` after it: the chunk's
/// older read can't land over the page's newer `w`. (Without the lock the
/// two would race inside the chunk's one statement: its update, finding the
/// page's committed row, would put its snapshot's `w` over the page's.)
#[tokio::test]
async fn a_page_queues_behind_a_field_chunk_holding_its_key() {
    let (mut d, plan) = start_field_build(&[(1, 1, 10), (2, 1, 20)]).await;
    let mut running = field_chunk_frozen(
        &d,
        &plan,
        None,
        "2",
        &[(PausePoint::AfterEntryLock, TARGET)],
    )
    .await;
    let frozen = running.reached(PausePoint::AfterEntryLock).await;
    let probe = d
        .ctl
        .execute(
            "select 1 from public.one__ledger where __from_key = '1' for update nowait",
            &[],
        )
        .await
        .expect_err("the field chunk holds key 1's entry");
    assert_eq!(
        probe.code(),
        Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE),
        "{probe}"
    );
    let user = d.user().await;
    user.batch_execute("update public.src set g = 3, v = 30 where id = 1")
        .await
        .expect("update key 1");
    let batch = d.seal().await;
    let page = d.drain_frozen(batch, "page", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut running, PausePoint::AfterEntryLock).await;
    assert_eq!(running.finish().await.keys, 2);
    page.finish().await;
    assert_eq!(
        d.rows(W_ACTUAL).await,
        d.rows(W_EXPECTED).await,
        "the page's later change wins, with no catch-up"
    );
    assert_field_oracle(&mut d).await;
}

/// A page frozen after its entry lock holds key 2, so a field chunk over it
/// gives up at its short lock timeout with `55P03` (transient), writing
/// nothing. Once the page commits, the chunk runs again.
#[tokio::test]
async fn a_field_chunk_gives_up_on_a_key_a_page_holds() {
    let (mut d, plan) = start_field_build(&[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    let user = d.user().await;
    user.batch_execute("update public.src set v = v + 10 where id = 2")
        .await
        .expect("update key 2");
    let batch = d.seal().await;
    let mut page = d
        .drain_frozen(batch, "page", &[(PausePoint::AfterEntryLock, TARGET)])
        .await;
    page.reached(PausePoint::AfterEntryLock).await;
    let err = field_chunk_frozen(&d, &plan, None, "3", &[])
        .await
        .finish_result()
        .await
        .expect_err("the chunk gives up on key 2");
    assert!(
        trellis::locks::is_lock_not_available(&err),
        "a lock timeout, not {err}"
    );
    assert_eq!(classify(&err), FailureClass::Transient);
    assert_eq!(
        d.rows("select count(*) from public.one where w is not null")
            .await,
        ["(0)"],
        "the chunk rolled back"
    );
    d.release(&mut page, PausePoint::AfterEntryLock).await;
    page.finish().await;
    assert_eq!(field_chunk(&d, &plan, None, "3").await.keys, 3);
    assert_field_oracle(&mut d).await;
}

/// A field paused while its build runs (an operator pause, say) is left out
/// of the build's chunks, as Apply leaves it out: it keeps the value it had.
#[tokio::test]
async fn a_field_chunk_leaves_a_paused_field_alone() {
    let (mut d, plan) = start_field_build(&[(1, 1, 1), (2, 1, 2)]).await;
    d.ctl
        .execute(
            "insert into column_status (transform_table, column_name, last_error, local_fuse) \
             values ('one', 'w', 'paused by the test', true)",
            &[],
        )
        .await
        .expect("pause one.w");
    let outcome = field_chunk(&d, &plan, None, "2").await;
    assert_eq!((outcome.keys, outcome.written), (2, 0));
    assert_eq!(
        d.rows("select count(*) from public.one where w is not null")
            .await,
        ["(0)"]
    );
    d.ctl
        .execute("delete from column_status", &[])
        .await
        .expect("unpause one.w");
    field_chunk(&d, &plan, None, "2").await;
    assert_field_oracle(&mut d).await;
}

/// The field chunk's statement reads the source and the target by their
/// keys, as the whole build's does
/// (`the_chunk_statement_reads_every_table_by_its_key`), and never the
/// ledger, which it doesn't write.
#[tokio::test]
async fn the_field_chunk_statement_reads_every_table_by_its_key() {
    let rows: Vec<(i32, i32, i32)> = (1..=50).map(|i| (i, i % 5, i)).collect();
    let (mut d, plan) = start_field_build(&rows).await;
    d.ctl
        .batch_execute("analyze public.src, public.one, public.one__ledger")
        .await
        .expect("analyze the tables");
    let keys: Vec<String> = (11..=20).map(|i| i.to_string()).collect();
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let mut client = d.pool().get().await.expect("a connection");
    let txn = client.transaction().await.expect("begin");
    let explained = one_to_one::explain_field_chunk(
        &txn,
        &plan,
        &["w".to_string()],
        Some("10"),
        "20",
        &key_refs,
    )
    .await
    .expect("explain the field chunk statement");
    txn.rollback().await.expect("roll back");
    drop(client);
    let scans: Vec<&str> = explained
        .lines()
        .filter(|line| line.contains("Seq Scan"))
        .collect();
    assert_eq!(
        scans,
        Vec::<&str>::new(),
        "no sequential scan:\n{explained}"
    );
    assert!(
        !explained.contains("one__ledger"),
        "the field chunk's statement doesn't touch the ledger:\n{explained}"
    );
    field_chunk(&d, &plan, None, "50").await;
    assert_field_oracle(&mut d).await;
}

// ------------------------------- a column resume against a page before it

/// A column resume releases its pause and registers the field build in one
/// commit (#625 F8b), and the column applies from there. A page that read
/// the paused columns before that commit leaves the column out of its
/// writes, so it must not apply after it. Otherwise this happens: the build
/// writes `dbl` of key 1; the page of an older change to key 1, computed
/// after the resume and drained out of order, writes `dbl` from its image;
/// and the frozen page, whose change is newer, passes I2 and writes key 1's
/// other columns, leaving `dbl` at the older change's value for good.
/// The resume bumps the source's version fence, as `ALTER TRANSFORM` does,
/// so it waits for that page, and a page that reaches the fence after it
/// misses and computes again, with the column.
#[tokio::test]
async fn a_page_computed_before_a_column_resume_does_not_apply_after_it() {
    let mut d = Driver::start(
        &format!("{CREATE} {}", seed(&[(1, 1, 1), (2, 1, 2)])),
        &columns(),
        &[ONE],
        &["public.src"],
    )
    .await;
    trellis::staging::quarantine::pause_column(d.pool(), "one", "dbl")
        .await
        .expect("pause one.dbl");
    let user = d.user().await;
    user.batch_execute("update public.src set v = 10 where id = 1")
        .await
        .expect("the older write");
    let older = d.seal().await;
    user.batch_execute("update public.src set v = 20, g = 2 where id = 1")
        .await
        .expect("the newer write");
    let newer = d.seal().await;
    // The newer batch's page computes with `dbl` paused, takes the version
    // fence, and stops before its entry lock.
    let mut page = d
        .drain_frozen(newer, "page", &[(PausePoint::AfterPlaceholders, TARGET)])
        .await;
    let frozen = page.reached(PausePoint::AfterPlaceholders).await;
    let mut resume = tokio::spawn({
        let pool = d.pool().clone();
        async move { trellis::staging::quarantine::resume_column(&pool, "one", "dbl").await }
    });
    let waited = tokio::select! {
        resumed = &mut resume => {
            resumed.expect("the resume's task").expect("resume one.dbl");
            false
        }
        () = d.wait_blocked_behind(frozen.backend_pid) => true,
    };
    if waited {
        d.release(&mut page, PausePoint::AfterPlaceholders).await;
        page.finish().await;
        resume
            .await
            .expect("the resume's task")
            .expect("resume one.dbl");
        trellis::staging::build::settle_builds(d.pool()).await;
        d.drain(older, "older").await;
    } else {
        // The race above, which the fence closes.
        trellis::staging::build::settle_builds(d.pool()).await;
        d.drain(older, "older").await;
        d.release(&mut page, PausePoint::AfterPlaceholders).await;
        page.finish().await;
    }
    assert!(
        waited,
        "the resume committed while a page computed with the column paused was in flight; \
         the target is now {:?} against the source's {:?}",
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
    );
    assert_oracle(&mut d).await;
}
