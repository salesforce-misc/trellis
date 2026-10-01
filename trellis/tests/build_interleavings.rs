//! Deterministic interleavings of the Re-derive build's primitives on the
//! real engine (#625 F1, epic #556, ADR-0002 "A build is Re-derive over
//! chunks"): build chunks (`trellis::staging::build::run_chunk`), the group
//! delta merger (`merge_deltas`), and Apply pages, driven by hand through
//! `support/drain_driver.rs`. No test sleeps or polls for convergence (#297).
//!
//! Nothing schedules a Re-derive build yet (#625 F2), so each test makes the
//! state a build starts from by hand: the definition is installed and
//! applying, and then its ledger, group deltas and target are emptied
//! ([`start_build`]). The source rows are then counted by nothing until a
//! chunk re-derives them, which is where a build starts: the definition
//! applies from the first chunk.
//!
//! Every test ends by checking the target against a from-scratch `GROUP BY`
//! over the source, after the last chunk, the last drain and the last merge.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::sync::{Arc, Mutex};

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::staging::build::{BuildPlan, ChunkOutcome};
use trellis::staging::interleave::PausePoint;

const TARGET: &str = "public.agg";

#[derive(Clone, Copy, Debug)]
enum Flavour {
    /// `SUM` and `COUNT(*)`.
    Sum,
    /// `AVG` beside a `SUM` sharing its hidden count, and `COUNT(x)`.
    Avg,
}

impl Flavour {
    const ALL: [Flavour; 2] = [Flavour::Sum, Flavour::Avg];

    fn definition(self) -> &'static str {
        match self {
            Flavour::Sum => {
                "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n"
            }
            Flavour::Avg => {
                "TRANSFORM agg FROM public.src GROUP BY g \
                 SELECT SUM(v) AS total, AVG(v) AS mean, COUNT(v) AS nv, COUNT(*) AS n"
            }
        }
    }

    fn actual(self) -> &'static str {
        match self {
            Flavour::Sum => "select g, total, n from public.agg order by g",
            Flavour::Avg => "select g, total, mean, nv, n from public.agg order by g",
        }
    }

    fn expected(self) -> &'static str {
        match self {
            Flavour::Sum => "select g, sum(v), count(*) from public.src group by g order by g",
            Flavour::Avg => {
                "select g, sum(v), avg(v), count(v), count(*) from public.src group by g order by g"
            }
        }
    }
}

/// `public.src (id, g, v)` created by `seed` (a statement inserting its
/// rows, or empty), one live `flavour` target over it, captured by triggers
/// and drained to quiescence, and then emptied of everything the build
/// writes: its ledger, its group deltas and its groups. Returns the driver
/// and the target's build plan.
async fn start_build_with(flavour: Flavour, seed: &str) -> (Driver, BuildPlan) {
    let d = Driver::start(
        &format!("create table public.src (id integer primary key, g integer, v numeric); {seed}"),
        &[
            ("id", ValueType::Numeric),
            ("g", ValueType::Numeric),
            ("v", ValueType::Numeric),
        ],
        &[flavour.definition()],
        &["public.src"],
    )
    .await;
    d.ctl
        .batch_execute("truncate public.agg__ledger, public.agg__deltas, public.agg")
        .await
        .expect("empty what the build writes");
    let plan = d.build_plan("agg").await;
    (d, plan)
}

/// [`start_build_with`] seeded with `rows`.
async fn start_build(flavour: Flavour, rows: &[(i32, i32, i32)]) -> (Driver, BuildPlan) {
    start_build_with(flavour, &seed(rows)).await
}

fn seed(rows: &[(i32, i32, i32)]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let values: Vec<String> = rows
        .iter()
        .map(|(id, g, v)| format!("({id}, {g}, {v})"))
        .collect();
    format!("insert into public.src values {};", values.join(", "))
}

/// Settles the pipeline, merges every delta row, and asserts the target
/// equals the oracle and no delta row is left.
async fn assert_oracle(d: &mut Driver, plan: &BuildPlan, flavour: Flavour) {
    d.settle().await;
    d.merge_all(plan).await;
    assert_eq!(deltas(d).await, 0, "every delta row is merged");
    let actual = d.rows(flavour.actual()).await;
    let expected = d.rows(flavour.expected()).await;
    assert_eq!(actual, expected, "{flavour:?}: target against the oracle");
}

/// The target's delta rows.
async fn deltas(d: &Driver) -> i64 {
    d.ctl
        .query_one("select count(*) from public.agg__deltas", &[])
        .await
        .expect("count the delta rows")
        .get(0)
}

/// Group `g`'s visible `total`, as text, and its hidden member count, or
/// `None` without a row.
async fn group(d: &Driver, g: i32) -> Option<(Option<String>, i64)> {
    d.ctl
        .query_opt(
            &format!("select total::text, __trellis_members from public.agg where g = {g}"),
            &[],
        )
        .await
        .expect("read the group")
        .map(|row| (row.get(0), row.get(1)))
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

// ------------------------------------------------- a chunk against an Apply

#[derive(Clone, Copy, Debug)]
enum Op {
    Insert,
    Update,
    Move,
    Delete,
}

impl Op {
    fn sql(self) -> &'static str {
        match self {
            Op::Insert => "insert into public.src values (7, 1, 70)",
            Op::Update => "update public.src set v = v + 10 where id = 2",
            Op::Move => "update public.src set g = 2 where id = 2",
            Op::Delete => "delete from public.src where id = 2",
        }
    }
}

/// A chunk over keys 1–10 and an Apply of `op` (on key 2, or key 7 for the
/// insert), in both orders:
///
/// - **Apply first.** The key has no entry, so it has been counted nowhere:
///   the Apply counts it from nothing (an insert of its new image, whatever
///   its old image says; a delete subtracts nothing and leaves a
///   tombstone). The chunk then finds the entry equal to the row and moves
///   nothing for it.
/// - **Chunk first.** The chunk counts the key into the group deltas, and
///   the Apply moves it from the entry the chunk wrote.
async fn chunk_against_apply(op: Op) {
    for flavour in Flavour::ALL {
        for chunk_first in [true, false] {
            let (mut d, plan) =
                start_build(flavour, &[(1, 1, 1), (2, 1, 2), (3, 2, 3), (4, 2, 4)]).await;
            let user = d.user().await;
            if chunk_first {
                d.chunk(&plan, None, "10").await;
            }
            user.batch_execute(op.sql()).await.expect("the write");
            let batch = d.seal().await;
            d.drain(batch, "apply").await;
            if !chunk_first {
                d.chunk(&plan, None, "10").await;
            }
            assert_oracle(&mut d, &plan, flavour).await;
        }
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
async fn a_chunk_and_an_apply_agree_on_a_group_move() {
    chunk_against_apply(Op::Move).await;
}

#[tokio::test]
async fn a_chunk_and_an_apply_agree_on_a_delete() {
    chunk_against_apply(Op::Delete).await;
}

// ------------------------------------------------------------ idempotence

/// A chunk run again finds every entry equal to its row and appends no delta
/// row, before or after the first run's deltas are merged.
#[tokio::test]
async fn a_chunk_run_again_writes_no_deltas() {
    for flavour in Flavour::ALL {
        let rows = [
            (1, 1, 1),
            (2, 1, 2),
            (3, 2, 3),
            (4, 2, 4),
            (5, 1, 5),
            (6, 2, 6),
        ];
        let (mut d, plan) = start_build(flavour, &rows).await;
        let first = d.chunk(&plan, None, "6").await;
        assert_eq!(
            first,
            ChunkOutcome {
                keys: 6,
                delta_rows: 2
            },
            "one delta row per group"
        );
        let again = d.chunk(&plan, None, "6").await;
        assert_eq!(
            again,
            ChunkOutcome {
                keys: 6,
                delta_rows: 0
            }
        );
        assert_eq!(deltas(&d).await, 2);
        d.merge_all(&plan).await;
        let after_merge = d.chunk(&plan, None, "6").await;
        assert_eq!(after_merge.delta_rows, 0);
        assert_oracle(&mut d, &plan, flavour).await;
    }
}

// ------------------------------------------------- chunked_read_exact_point

/// 623 Q1's scenario over real chunks: chunk 1 is keys 1–3, chunk 2 keys
/// 4–6, each at its own snapshot.
///
/// - W_in is in flight across both chunks: it updates key 2 (chunk 1) and
///   key 5 (chunk 2) before chunk 1 runs and commits after chunk 2's read.
///   Neither read sees it, so both of its changes must apply.
/// - W_between commits between the chunks: it updates key 1 (chunk 1,
///   already read: must apply) and key 4 (chunk 2, not yet read: chunk 2
///   sees it, so it must not count twice), and moves key 6 (chunk 2) from
///   group 2 to group 1.
///
/// Chunk 2 is frozen after its read-and-write statement while W_in commits,
/// and the writers' CDC drains on a page that must queue behind chunk 2's
/// entry locks (I1), and then applies exactly the changes chunk 2 didn't see.
#[tokio::test]
async fn chunked_read_exact_point_over_build_chunks() {
    let flavour = Flavour::Sum;
    let rows = [
        (1, 1, 1),
        (2, 1, 2),
        (3, 2, 3),
        (4, 1, 4),
        (5, 1, 5),
        (6, 2, 6),
    ];
    let (mut d, plan) = start_build(flavour, &rows).await;

    let w_in = d.user().await;
    w_in.batch_execute(
        "begin; \
         update public.src set v = v + 100 where id = 2; \
         update public.src set v = v + 1000 where id = 5",
    )
    .await
    .expect("open W_in");
    let w_in_xid = xact_id(&w_in).await;
    d.chunk(&plan, None, "3").await;
    let w_between = d.user().await;
    w_between
        .batch_execute(
            "begin; \
             update public.src set v = v + 10 where id = 1; \
             update public.src set v = v + 20 where id = 4; \
             update public.src set g = 1 where id = 6",
        )
        .await
        .expect("open W_between");
    let w_between_xid = xact_id(&w_between).await;
    w_between
        .batch_execute("commit")
        .await
        .expect("commit W_between");
    let mut chunk_2 = d
        .chunk_frozen(
            &plan,
            Some("3"),
            "6",
            &[(PausePoint::AfterRederiveRead, TARGET)],
        )
        .await;
    let frozen = chunk_2.reached(PausePoint::AfterRederiveRead).await;
    w_in.batch_execute("commit").await.expect("commit W_in");

    let cdc = d.seal().await;
    let cdc = d.drain_frozen(cdc, "cdc", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut chunk_2, PausePoint::AfterRederiveRead).await;
    chunk_2.finish().await;
    cdc.finish().await;

    let entries: Vec<(String, bool, bool, Option<String>)> = d
        .ctl
        .query(
            "select __from_key, \
                    pg_visible_in_snapshot($1::text::xid8, __basis), \
                    pg_visible_in_snapshot($2::text::xid8, __basis), \
                    __applied_lsn::text \
             from public.agg__ledger order by __from_key",
            &[&w_in_xid, &w_between_xid],
        )
        .await
        .expect("read the entries")
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    assert_eq!(entries.len(), 6, "{entries:?}");
    for (key, w_in_seen, w_between_seen, _) in &entries {
        assert!(
            !w_in_seen,
            "W_in is in flight across both chunks, key {key}"
        );
        assert_eq!(
            *w_between_seen,
            key.as_str() >= "4",
            "W_between commits between the chunks, key {key}: {entries:?}"
        );
    }
    assert_eq!(
        entries[2].3, None,
        "key 3 was only ever re-derived, which leaves applied_lsn alone"
    );
    assert_oracle(&mut d, &plan, flavour).await;
}

/// The open transaction's id on `client`, as text.
async fn xact_id(client: &tokio_postgres::Client) -> String {
    client
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("read the transaction id")
        .get(0)
}

// ------------------------------------------------------ two group channels

/// #625 finding 5. A chunk counts K′ (group 1, v 3) into a delta row that
/// isn't merged yet. Then one page inserts K (group 1, v 5) and moves K′ to
/// group 2, writing group 1 directly: members 0 and a sum of 2. That row
/// owes the pending delta its sum, so it must survive (B5: a group goes only
/// when every accumulator is 0), with its sum, not `NULL`, and the merge
/// must leave it equal to the oracle: (5, 1), not (3, 1).
///
/// Twice: once with group 1 created by the page (the upsert's insert arm),
/// and once with it already there, holding a key A (v 1) an earlier chunk
/// counted and merged, which the page deletes (its update arm).
#[tokio::test]
async fn a_group_at_zero_members_keeps_the_sum_it_is_owed() {
    for flavour in Flavour::ALL {
        for existing in [false, true] {
            let (mut d, plan) = start_build(flavour, &[(2, 1, 3), (3, 1, 1)]).await;
            if existing {
                d.chunk(&plan, Some("2"), "3").await;
                d.merge_all(&plan).await;
                assert_eq!(group(&d, 1).await, Some((Some("1".to_string()), 1)));
            } else {
                d.ctl
                    .batch_execute("delete from public.src where id = 3")
                    .await
                    .expect("no key A");
                d.settle().await;
            }
            d.chunk(&plan, None, "2").await;
            assert_eq!(deltas(&d).await, 1);
            let user = d.user().await;
            user.batch_execute(
                "begin; \
                 delete from public.src where id = 3; \
                 insert into public.src values (1, 1, 5); \
                 update public.src set g = 2 where id = 2; \
                 commit",
            )
            .await
            .expect("the page's writes");
            let batch = d.seal().await;
            d.drain(batch, "apply").await;
            assert_eq!(
                group(&d, 1).await,
                Some((Some("2".to_string()), 0)),
                "{flavour:?}, existing {existing}: group 1 at members 0 keeps its sum"
            );
            assert_oracle(&mut d, &plan, flavour).await;
            assert_eq!(group(&d, 1).await, Some((Some("5".to_string()), 1)));
        }
    }
}

/// A chunk counts K′ into group 1's pending delta, and a page then moves K′
/// to group 2, taking it out of a group row that never counted it: group 1
/// is transiently negative. The merge brings it to all zeros, and it goes.
#[tokio::test]
async fn a_transiently_negative_group_goes_once_merged() {
    for flavour in Flavour::ALL {
        let (mut d, plan) = start_build(flavour, &[(2, 1, 3)]).await;
        d.chunk(&plan, None, "2").await;
        let user = d.user().await;
        user.batch_execute("update public.src set g = 2 where id = 2")
            .await
            .expect("move K′");
        let batch = d.seal().await;
        d.drain(batch, "apply").await;
        assert_eq!(
            group(&d, 1).await,
            Some((Some("-3".to_string()), -1)),
            "{flavour:?}: group 1 is negative until the merge"
        );
        let merged = d.merge(&plan, 100).await;
        assert_eq!((merged.claimed, merged.written, merged.deleted), (1, 0, 1));
        assert_eq!(group(&d, 1).await, None);
        assert_oracle(&mut d, &plan, flavour).await;
    }
}

// --------------------------------------------------------------- deadlocks

/// Two mergers on overlapping groups, and a page on the same groups, never
/// deadlock: a merger claims delta rows without waiting (`skip locked`) and
/// upserts its groups in group order, and a page locks its entries and then
/// its groups in the same order.
///
/// First forced: merger A is frozen holding every group, merger B claims
/// the rows A didn't and queues on A's groups, and a page queues on them
/// too. Then a free-running round: chunk runners, two merger loops and pages
/// at once, with the source moving under them.
///
/// A deadlock shows as the `deadlock detected` error in this test's own
/// cluster log ([`Driver::deadlocks_logged`]), which the aborted backend
/// writes before its client sees the error, so the check is exact when the
/// last transaction has finished. `pg_stat_database.deadlocks` is not used:
/// a backend reports it only when its pending stats are next flushed, so it
/// can lag (#297).
#[tokio::test]
async fn mergers_and_pages_on_the_same_groups_never_deadlock() {
    let flavour = Flavour::Sum;
    let (mut d, plan) = start_build_with(
        flavour,
        "insert into public.src select i, i % 20, i from generate_series(1, 2000) i;",
    )
    .await;

    // Forced.
    d.chunk(&plan, None, "100").await;
    let mut merger_a = d
        .merge_frozen(&plan, 1_000, &[(PausePoint::AfterGroupUpsert, TARGET)])
        .await;
    let frozen = merger_a.reached(PausePoint::AfterGroupUpsert).await;
    d.chunk(&plan, Some("100"), "200").await;
    let merger_b = d.merge_frozen(&plan, 1_000, &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    let user = d.user().await;
    user.batch_execute("update public.src set v = v + 1 where id <= 200")
        .await
        .expect("touch every group");
    let batch = d.seal().await;
    let page = d.drain_frozen(batch, "page", &[]).await;
    d.wait_blocked(2).await;
    d.release(&mut merger_a, PausePoint::AfterGroupUpsert).await;
    assert_eq!(merger_a.finish().await.claimed, 20);
    assert_eq!(merger_b.finish().await.claimed, 20);
    page.finish().await;

    // Free-running: per round, three chunks and two mergers at once, with a
    // write across every group drained on a page beside them.
    let ranges: Vec<(String, String)> = (2..20)
        .map(|i| ((i * 100).to_string(), ((i + 1) * 100).to_string()))
        .collect();
    for (round, three) in ranges.chunks(3).enumerate() {
        let mut chunks = Vec::new();
        for (lo, hi) in three {
            chunks.push(d.chunk_frozen(&plan, Some(lo), hi, &[]).await);
        }
        let mergers = [
            d.merge_frozen(&plan, 7, &[]).await,
            d.merge_frozen(&plan, 7, &[]).await,
        ];
        user.batch_execute(&format!(
            "update public.src set v = v + 1 where id % 6 = {round}"
        ))
        .await
        .expect("a write across every group");
        let batch = d.seal().await;
        d.drain(batch, "page").await;
        for chunk in chunks {
            chunk.finish().await;
        }
        for merger in mergers {
            merger.finish().await;
        }
    }

    assert_eq!(d.deadlocks_logged(), Vec::<String>::new());
    assert_oracle(&mut d, &plan, flavour).await;
}

// ---------------------------------------------------------------- truncate

/// A source `TRUNCATE` empties the ledger, and the delta rows go with it
/// (B4): they record moves between entries the truncate discarded.
#[tokio::test]
async fn a_source_truncate_discards_the_pending_deltas() {
    let flavour = Flavour::Sum;
    let (mut d, plan) = start_build(flavour, &[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    d.chunk(&plan, None, "3").await;
    assert_eq!(deltas(&d).await, 2);
    let user = d.user().await;
    user.batch_execute("truncate public.src")
        .await
        .expect("truncate");
    let batch = d.seal().await;
    d.drain(batch, "apply").await;
    assert_eq!(deltas(&d).await, 0, "the truncate discards the deltas");
    assert_eq!(d.rows("select * from public.agg__ledger").await.len(), 0);
    user.batch_execute("insert into public.src values (4, 1, 40), (5, 3, 50)")
        .await
        .expect("insert after the truncate");
    assert_oracle(&mut d, &plan, flavour).await;
}

/// The one-pass build empties the ledger and writes it again from the
/// source, so it discards the pending delta rows with the old entries (B4).
#[tokio::test]
async fn the_one_pass_build_discards_the_pending_deltas() {
    let flavour = Flavour::Sum;
    let (mut d, plan) = start_build(flavour, &[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    d.chunk(&plan, None, "3").await;
    assert_eq!(deltas(&d).await, 2);
    let definition = trellis::defs::catalog::definition_by_target(d.pool(), "agg")
        .await
        .expect("read the definition")
        .expect("the definition");
    trellis::defs::backfill_definition(
        d.pool(),
        &definition.def,
        "public",
        &definition.source_table,
        &definition.source_columns,
    )
    .await
    .expect("the one-pass build");
    assert_eq!(deltas(&d).await, 0, "the build discards the deltas");
    assert_oracle(&mut d, &plan, flavour).await;
}

// -------------------------------------------------------------- a rebuild

/// A build over entries that disagree with the source (changes no entry
/// heard about: values, a group move, a new row) converges through the
/// group deltas alone. Nothing empties the ledger first: each chunk moves
/// every stale entry by its difference, and only the merge writes groups.
///
/// A key deleted while the entries were stale isn't in any chunk's range;
/// its entry is the sweep's (#625 F3), so this leaves deletes out.
#[tokio::test]
async fn a_rebuild_over_stale_entries_converges_through_deltas_alone() {
    for flavour in Flavour::ALL {
        let rows = [(1, 1, 1), (2, 1, 2), (3, 2, 3), (4, 2, 4), (5, 1, 5)];
        let d = Driver::start(
            &format!(
                "create table public.src (id integer primary key, g integer, v numeric); {}",
                seed(&rows)
            ),
            &[
                ("id", ValueType::Numeric),
                ("g", ValueType::Numeric),
                ("v", ValueType::Numeric),
            ],
            &[flavour.definition()],
            &["public.src"],
        )
        .await;
        let mut d = d;
        let plan = d.build_plan("agg").await;
        write_uncaptured(
            &d,
            "update public.src set v = v + 5 where id in (1, 2); \
             update public.src set g = 3 where id = 4; \
             insert into public.src values (6, 2, 60)",
        )
        .await;
        let stale = d.rows(flavour.actual()).await;
        assert_ne!(stale, d.rows(flavour.expected()).await);

        d.chunk(&plan, None, "3").await;
        d.chunk(&plan, Some("3"), "6").await;
        assert_eq!(
            d.rows(flavour.actual()).await,
            stale,
            "{flavour:?}: chunks write no group"
        );
        assert!(deltas(&d).await > 0);
        assert_oracle(&mut d, &plan, flavour).await;
    }
}

// ------------------------------------------------------- chunk lock waits

/// A chunk frozen after its entry lock holds off a page on one of its keys:
/// the page queues, and proceeds once the chunk commits.
#[tokio::test]
async fn a_page_queues_behind_a_chunk_holding_its_key() {
    let flavour = Flavour::Sum;
    let (mut d, plan) = start_build(flavour, &[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    let mut chunk = d
        .chunk_frozen(&plan, None, "3", &[(PausePoint::AfterEntryLock, TARGET)])
        .await;
    let frozen = chunk.reached(PausePoint::AfterEntryLock).await;
    let user = d.user().await;
    user.batch_execute("update public.src set v = v + 10 where id = 2")
        .await
        .expect("update key 2");
    let batch = d.seal().await;
    let page = d.drain_frozen(batch, "page", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut chunk, PausePoint::AfterEntryLock).await;
    assert_eq!(chunk.finish().await.keys, 3);
    page.finish().await;
    assert_oracle(&mut d, &plan, flavour).await;
}

/// A page frozen after its entry lock holds key 2, so a chunk over key 2
/// gives up at its own short lock timeout (`CHUNK_LOCK_TIMEOUT`) with
/// `55P03`, writing nothing, instead of holding up pages behind it. Once
/// the page commits, the chunk runs again and the build converges.
#[tokio::test]
async fn a_chunk_gives_up_on_a_key_a_page_holds() {
    let flavour = Flavour::Sum;
    let (mut d, plan) = start_build(flavour, &[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    let user = d.user().await;
    user.batch_execute("update public.src set v = v + 10 where id = 2")
        .await
        .expect("update key 2");
    let batch = d.seal().await;
    let mut page = d
        .drain_frozen(batch, "page", &[(PausePoint::AfterEntryLock, TARGET)])
        .await;
    page.reached(PausePoint::AfterEntryLock).await;
    let err = d
        .chunk_frozen(&plan, None, "3", &[])
        .await
        .finish_result()
        .await
        .expect_err("the chunk gives up on key 2");
    assert!(
        trellis::locks::is_lock_not_available(&err),
        "a lock timeout, not {err}"
    );
    assert_eq!(deltas(&d).await, 0, "the chunk rolled back");
    d.release(&mut page, PausePoint::AfterEntryLock).await;
    page.finish().await;
    assert_eq!(d.chunk(&plan, None, "3").await.keys, 3);
    assert_oracle(&mut d, &plan, flavour).await;
}

// ------------------------------------------------------- tombstone GC (D7)

/// A chunk's tombstone (a key it locked whose row was deleted before its
/// read) carries the segment that was active at its read (#625 finding 11),
/// so D7's collection keeps it while that segment is undrained, and takes it
/// once it drains.
#[tokio::test]
async fn a_chunk_tombstone_is_collected_once_its_segment_drains() {
    let flavour = Flavour::Sum;
    let (mut d, plan) = start_build(flavour, &[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    let mut chunk = d
        .chunk_frozen(&plan, None, "3", &[(PausePoint::AfterPlaceholders, TARGET)])
        .await;
    chunk.reached(PausePoint::AfterPlaceholders).await;
    let user = d.user().await;
    user.batch_execute("delete from public.src where id = 3")
        .await
        .expect("delete key 3 under the chunk");
    d.release(&mut chunk, PausePoint::AfterPlaceholders).await;
    chunk.finish().await;

    let active: i64 = d
        .ctl
        .query_one("select max(seg_seq) from segments", &[])
        .await
        .expect("read the active segment")
        .get(0);
    assert_eq!(key_3_entry(&d).await, Some((true, Some(active))));

    d.collect_tombstones().await;
    assert!(
        key_3_entry(&d).await.is_some(),
        "kept while its segment is undrained"
    );
    let batch = d.seal().await;
    assert_eq!(batch, active, "the delete was captured into that segment");
    d.drain(batch, "apply").await;
    d.collect_tombstones().await;
    assert_eq!(key_3_entry(&d).await, None, "collected once it drained");
    assert_oracle(&mut d, &plan, flavour).await;
}

/// Key 3's entry: whether it is a tombstone, and its `applied_seg`.
async fn key_3_entry(d: &Driver) -> Option<(bool, Option<i64>)> {
    d.ctl
        .query_opt(
            "select __tombstone, __applied_seg from public.agg__ledger \
             where __from_key = '3'",
            &[],
        )
        .await
        .expect("read key 3's entry")
        .map(|r| (r.get(0), r.get(1)))
}

// ------------------------------------------ the entry lock's retake (#712)

/// A build whose ledger holds key 2's tombstone, already collectible, and
/// whose source has key 2 back: every key chunked, key 2 deleted and
/// drained, then key 1 updated and key 2 re-inserted, captured into the
/// returned batch, which is left undrained.
async fn start_retake(flavour: Flavour) -> (Driver, BuildPlan, i64) {
    let (mut d, plan) = start_build(flavour, &[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
    d.chunk(&plan, None, "3").await;
    let user = d.user().await;
    user.batch_execute("delete from public.src where id = 2")
        .await
        .expect("delete key 2");
    let deleted = d.seal().await;
    d.drain(deleted, "apply").await;
    user.batch_execute(
        "update public.src set v = 11 where id = 1; insert into public.src values (2, 1, 25)",
    )
    .await
    .expect("update key 1, re-insert key 2");
    let batch = d.seal().await;
    (d, plan, batch)
}

/// A page retaking its entry lock gives back the lock it already holds
/// before inserting a placeholder again, so a chunk that took the collected
/// key's placeholder in the meantime gets every lock it asks for. The page
/// (keys 1 and 2) is frozen after its placeholder insert, which found key
/// 2's tombstone. The GC collects it, and the chunk (keys 1 to 3) inserts
/// key 2's placeholder and is frozen there. The page locks key 1, finds no
/// entry for key 2, retakes, and queues on the chunk's placeholder; then
/// the chunk locks its keys. Had the page kept key 1's lock, the chunk would
/// queue on it: a deadlock, or the chunk giving up at its lock timeout.
#[tokio::test]
async fn a_page_retaking_its_entry_lock_never_deadlocks_with_a_chunk() {
    let flavour = Flavour::Sum;
    let (mut d, plan, batch) = start_retake(flavour).await;
    let mut page = d
        .drain_frozen(batch, "page", &[(PausePoint::AfterPlaceholders, TARGET)])
        .await;
    page.reached(PausePoint::AfterPlaceholders).await;
    assert_eq!(d.collect_tombstones().await, 1, "key 2's tombstone goes");
    let mut chunk = d
        .chunk_frozen(&plan, None, "3", &[(PausePoint::AfterPlaceholders, TARGET)])
        .await;
    let frozen = chunk.reached(PausePoint::AfterPlaceholders).await;
    d.release(&mut page, PausePoint::AfterPlaceholders).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut chunk, PausePoint::AfterPlaceholders).await;
    let chunked = chunk.finish_result().await;
    let paged = page.finish_result().await;

    assert_eq!(d.deadlocks_logged(), Vec::<String>::new());
    assert_eq!(chunked.expect("the chunk takes every lock").keys, 3);
    paged.expect("the page");
    assert_oracle(&mut d, &plan, flavour).await;
}

/// A chunk retaking its entry lock keeps its own short lock timeout: the
/// savepoint the retake rolls back to is taken after the chunk set it. The
/// chunk (keys 1 to 3) is frozen after its placeholder insert, which found
/// key 2's tombstone. The GC collects it, and a page inserts key 2's
/// placeholder and is frozen there, holding it. The chunk retakes, queues
/// on that placeholder, and gives up at its own timeout rather than the
/// page session's two minutes.
#[tokio::test]
async fn a_chunk_retaking_its_entry_lock_keeps_its_lock_timeout() {
    let flavour = Flavour::Sum;
    let (mut d, plan, batch) = start_retake(flavour).await;
    let mut chunk = d
        .chunk_frozen(&plan, None, "3", &[(PausePoint::AfterPlaceholders, TARGET)])
        .await;
    chunk.reached(PausePoint::AfterPlaceholders).await;
    assert_eq!(d.collect_tombstones().await, 1, "key 2's tombstone goes");
    let mut page = d
        .drain_frozen(batch, "page", &[(PausePoint::AfterPlaceholders, TARGET)])
        .await;
    page.reached(PausePoint::AfterPlaceholders).await;
    d.release(&mut chunk, PausePoint::AfterPlaceholders).await;
    let err = tokio::time::timeout(std::time::Duration::from_secs(30), chunk.finish_result())
        .await
        .expect("the chunk gives up at its own lock timeout, not the session's")
        .expect_err("the chunk gives up on key 2");
    assert!(
        trellis::locks::is_lock_not_available(&err),
        "a lock timeout, not {err}"
    );
    d.release(&mut page, PausePoint::AfterPlaceholders).await;
    page.finish().await;
    assert_eq!(d.chunk(&plan, None, "3").await.keys, 3);
    assert_eq!(d.deadlocks_logged(), Vec::<String>::new());
    assert_oracle(&mut d, &plan, flavour).await;
}

// ------------------------------------------------ a build under writers

/// A chunk's `(lo, hi]`, as encoded keys.
type Range = (Option<String>, String);

/// A 200k-row build by 8 concurrent hand-driven chunk runners, under two
/// writers inserting, updating, moving and deleting across the key space,
/// with pages and merges between, equals a from-scratch `GROUP BY`. A chunk
/// that gives up on a key a page holds goes back on the queue.
#[tokio::test]
async fn a_build_by_eight_chunk_runners_under_writers_equals_the_oracle() {
    const ROWS: i64 = 200_000;
    const CHUNK_ROWS: i64 = 10_000;
    let flavour = Flavour::Avg;
    let (mut d, plan) = start_build_with(
        flavour,
        &format!(
            "insert into public.src select i, i % 1000, i % 97 \
             from generate_series(1, {ROWS}) i;"
        ),
    )
    .await;

    let queue: Arc<Mutex<Vec<Range>>> = Arc::new(Mutex::new(
        (0..ROWS / CHUNK_ROWS)
            .map(|i| {
                (
                    (i > 0).then(|| (i * CHUNK_ROWS).to_string()),
                    ((i + 1) * CHUNK_ROWS).to_string(),
                )
            })
            .collect(),
    ));
    let mut runners = Vec::new();
    for _ in 0..8 {
        let queue = Arc::clone(&queue);
        let pool = d.pool().clone();
        let plan = plan.clone();
        runners.push(tokio::spawn(async move {
            let mut retries = 0;
            loop {
                let Some((lo, hi)) = queue.lock().expect("queue").pop() else {
                    return retries;
                };
                let mut client = pool.get().await.expect("pool");
                let txn = client.transaction().await.expect("begin");
                match trellis::staging::build::run_chunk(&txn, &plan, lo.as_deref(), &hi).await {
                    Ok(_) => txn.commit().await.expect("commit the chunk"),
                    Err(err) if trellis::locks::is_lock_not_available(&err) => {
                        drop(txn);
                        retries += 1;
                        queue.lock().expect("queue").push((lo, hi));
                    }
                    Err(err) => panic!("chunk ({lo:?}, {hi}]: {err}"),
                }
            }
        }));
    }
    let mut writers = Vec::new();
    for w in 0..2_i64 {
        let user = d.user().await;
        writers.push(tokio::spawn(async move {
            // A fixed walk over the key space per writer.
            let mut x: i64 = 7 + w;
            for i in 0..300_i64 {
                x = (x * 48_271 + 11) % 2_147_483_647;
                let id = x % (ROWS + 1_000) + 1;
                let sql = match i % 4 {
                    0 => format!("update public.src set v = v + 1 where id = {id}"),
                    1 => format!("update public.src set g = {} where id = {id}", x % 1000),
                    2 => format!("delete from public.src where id = {id}"),
                    _ => format!(
                        "insert into public.src values ({id}, {}, {}) on conflict do nothing",
                        x % 1000,
                        x % 89
                    ),
                };
                user.batch_execute(&sql)
                    .await
                    .expect("a writer's statement");
            }
        }));
    }
    // Pages and merges until every chunk is done. A seal the gate refuses
    // (a chunk transaction straddling it) is skipped this time round.
    while !runners.iter().all(|r| r.is_finished()) {
        if let Some(batch) = d.try_seal().await {
            d.drain(batch, "page").await;
        }
        d.merge(&plan, 5_000).await;
    }
    for writer in writers {
        writer.await.expect("writer");
    }
    for runner in runners {
        runner.await.expect("chunk runner");
    }
    assert!(queue.lock().expect("queue").is_empty());
    assert_oracle(&mut d, &plan, flavour).await;
}
