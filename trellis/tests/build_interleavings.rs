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
use std::time::Duration;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::staging::apply::ApplyError;
use trellis::staging::build::{self, BuildPlan, ChunkOutcome, MergeOutcome};
use trellis::staging::interleave::PausePoint;
use trellis::staging::quarantine::{FailureClass, classify};

const TARGET: &str = "public.agg";

#[derive(Clone, Copy, Debug)]
enum Flavour {
    /// `SUM` and `COUNT(*)`.
    Sum,
    /// `AVG` beside a `SUM` sharing its hidden count, and `COUNT(x)`.
    Avg,
    /// `MIN` and `MAX`, recomputed fields that fold (#625 F5).
    MinMax,
    /// `BOOL_AND` and `BOOL_OR` of a generated column, and a `MAX`: the
    /// other folding fields (#625 F5).
    Bool,
    /// A composed field and expression arguments: recomputed from every
    /// entry of the group, never folded (#625 F5).
    Composed,
}

impl Flavour {
    /// The flavours whose target has a `total` ([`group`]).
    const ALL: [Flavour; 2] = [Flavour::Sum, Flavour::Avg];

    /// Every flavour, the recomputing ones included.
    const EVERY: [Flavour; 5] = [
        Flavour::Sum,
        Flavour::Avg,
        Flavour::MinMax,
        Flavour::Bool,
        Flavour::Composed,
    ];

    fn definition(self) -> &'static str {
        match self {
            Flavour::Sum => {
                "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n"
            }
            Flavour::Avg => {
                "TRANSFORM agg FROM public.src GROUP BY g \
                 SELECT SUM(v) AS total, AVG(v) AS mean, COUNT(v) AS nv, COUNT(*) AS n"
            }
            Flavour::MinMax => {
                "TRANSFORM agg FROM public.src GROUP BY g \
                 SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n"
            }
            Flavour::Bool => {
                "TRANSFORM agg FROM public.src GROUP BY g \
                 SELECT BOOL_AND(b) AS all_b, BOOL_OR(b) AS any_b, MAX(v) AS hi, COUNT(*) AS n"
            }
            Flavour::Composed => {
                "TRANSFORM agg FROM public.src GROUP BY g \
                 SELECT MAX(v) + MIN(v) AS ends, MAX(v + v + 1) AS top, \
                        SUM(v + 3) AS plus3, COUNT(*) AS n"
            }
        }
    }

    fn actual(self) -> &'static str {
        match self {
            Flavour::Sum => "select g, total, n from public.agg order by g",
            Flavour::Avg => "select g, total, mean, nv, n from public.agg order by g",
            Flavour::MinMax => "select g, lo, hi, n from public.agg order by g",
            Flavour::Bool => "select g, all_b, any_b, hi, n from public.agg order by g",
            Flavour::Composed => "select g, ends, top, plus3, n from public.agg order by g",
        }
    }

    fn expected(self) -> &'static str {
        match self {
            Flavour::Sum => "select g, sum(v), count(*) from public.src group by g order by g",
            Flavour::Avg => {
                "select g, sum(v), avg(v), count(v), count(*) from public.src group by g order by g"
            }
            Flavour::MinMax => {
                "select g, min(v), max(v), count(*) from public.src group by g order by g"
            }
            Flavour::Bool => {
                "select g, bool_and(b), bool_or(b), max(v), count(*) \
                 from public.src group by g order by g"
            }
            Flavour::Composed => {
                "select g, max(v) + min(v), max(v + v + 1), sum(v + 3), count(*) \
                 from public.src group by g order by g"
            }
        }
    }

    /// The source table's DDL: `(id, g, v)`, and for [`Flavour::Bool`] a
    /// `b` generated from `v`, so every seed and write here stays three
    /// values.
    fn create_source(self) -> &'static str {
        match self {
            Flavour::Bool => {
                "create table public.src (id integer primary key, g integer, v numeric, \
                 b boolean generated always as (v > 3) stored);"
            }
            _ => "create table public.src (id integer primary key, g integer, v numeric);",
        }
    }

    fn columns(self) -> Vec<(&'static str, ValueType)> {
        let mut columns = vec![
            ("id", ValueType::Numeric),
            ("g", ValueType::Numeric),
            ("v", ValueType::Numeric),
        ];
        if matches!(self, Flavour::Bool) {
            columns.push(("b", ValueType::Boolean));
        }
        columns
    }
}

/// `public.src (id, g, v)` created by `seed` (a statement inserting its
/// rows, or empty), one live `flavour` target over it, captured by triggers
/// and drained to quiescence, and then emptied of everything the build
/// writes: its ledger, its group deltas and its groups. Returns the driver
/// and the target's build plan.
async fn start_build_with(flavour: Flavour, seed: &str) -> (Driver, BuildPlan) {
    let d = Driver::start(
        &format!("{} {seed}", flavour.create_source()),
        &flavour.columns(),
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

/// Merges every delta row, a partition per pass (#717), and returns the
/// passes' outcomes summed: each of `claimed`, `written`, `deleted`,
/// `folded` and `recomputed` over every group of every partition.
async fn merge_every(d: &Driver, plan: &BuildPlan) -> MergeOutcome {
    let mut total = MergeOutcome {
        partition: None,
        claimed: 0,
        written: 0,
        deleted: 0,
        folded: 0,
        recomputed: 0,
        skipped: false,
    };
    loop {
        let pass = d.merge(plan, 1_000).await;
        assert!(!pass.skipped, "no other merger runs: {pass:?}");
        if pass.claimed == 0 {
            return total;
        }
        total.claimed += pass.claimed;
        total.written += pass.written;
        total.deleted += pass.deleted;
        total.folded += pass.folded;
        total.recomputed += pass.recomputed;
    }
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
    for flavour in Flavour::EVERY {
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
    for flavour in Flavour::EVERY {
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
        let merged = merge_every(&d, &plan).await;
        assert_eq!((merged.claimed, merged.written, merged.deleted), (1, 0, 1));
        assert_eq!(group(&d, 1).await, None);
        assert_oracle(&mut d, &plan, flavour).await;
    }
}

// ------------------------------------------- recomputed fields (#625 F5)

/// Group `g`'s `lo`, `hi` (as text) and `n` under [`Flavour::MinMax`], or
/// `None` without a row.
async fn min_max(d: &Driver, g: i32) -> Option<(Option<String>, Option<String>, i64)> {
    d.ctl
        .query_opt(
            &format!("select lo::text, hi::text, n from public.agg where g = {g}"),
            &[],
        )
        .await
        .expect("read the group")
        .map(|row| (row.get(0), row.get(1), row.get(2)))
}

fn mm(lo: i32, hi: i32, n: i64) -> Option<(Option<String>, Option<String>, i64)> {
    Some((Some(lo.to_string()), Some(hi.to_string()), n))
}

/// Exp 2 scenario 5's `MIN`/`MAX` variant during a build (#494; #623 D4,
/// #625 F5). Key 1 is counted into group a by an early chunk and merged.
/// A chunk over keys 5 (group z) and 6 (group b) is frozen after its
/// read-and-write. Meanwhile key 1 moves a → z (C1) and then z → b (C2),
/// and the folded record, a → b, drains while the chunk is still open: the
/// page writes b from its entries, which can't see the chunk's key 6 yet.
/// The chunk commits, and the merge folds key 6 into b's stored values and
/// creates z. Every group ends equal to the oracle: z without key 1, and b
/// with both keys.
#[tokio::test]
async fn exp2_5_min_max_during_a_build() {
    let flavour = Flavour::MinMax;
    let (mut d, plan) = start_build(flavour, &[(1, 1, 10), (5, 2, 50), (6, 3, 60)]).await;
    d.chunk(&plan, None, "1").await;
    d.merge_all(&plan).await;
    assert_eq!(min_max(&d, 1).await, mm(10, 10, 1));

    let user = d.user().await;
    let mut chunk = d
        .chunk_frozen(
            &plan,
            Some("1"),
            "6",
            &[(PausePoint::AfterRederiveRead, TARGET)],
        )
        .await;
    chunk.reached(PausePoint::AfterRederiveRead).await;
    user.batch_execute("update public.src set g = 2 where id = 1")
        .await
        .expect("C1");
    user.batch_execute("update public.src set g = 3 where id = 1")
        .await
        .expect("C2");
    let batch = d.seal().await;
    d.drain(batch, "page").await;
    assert_eq!(min_max(&d, 1).await, None, "a is empty");
    assert_eq!(
        min_max(&d, 3).await,
        mm(10, 10, 1),
        "the page's b sees only key 1: the chunk's key 6 isn't committed"
    );
    d.release(&mut chunk, PausePoint::AfterRederiveRead).await;
    chunk.finish().await;

    let merged = merge_every(&d, &plan).await;
    assert_eq!(
        (
            merged.claimed,
            merged.written,
            merged.folded,
            merged.recomputed
        ),
        (2, 2, 1, 1),
        "b folds key 6 into its row; z is created, so recomputed"
    );
    assert_eq!(min_max(&d, 2).await, mm(50, 50, 1), "z without key 1");
    assert_eq!(min_max(&d, 3).await, mm(10, 60, 2));
    assert_oracle(&mut d, &plan, flavour).await;
}

/// Deleting a group's current `MIN` and `MAX` forces a real recompute
/// while a build runs (generative `convergence.rs`'s
/// `deleting_a_groups_current_min_and_max_forces_a_real_recompute`, #625
/// F5), through either channel:
///
/// - **A page.** The group's extremes, keys 2 (20) and 3 (1), are deleted
///   by captured writes while a chunk's delta for key 4 (10) is pending: the
///   page recomputes the group from its entries, key 4's included, and the
///   merge then folds key 4 in again, which changes nothing.
/// - **A chunk.** The same deletes, uncaptured, are only seen by a rebuild's
///   chunk, which retires keys 2 and 3: its delta row says values left the
///   group, so the merge recomputes it rather than fold.
#[tokio::test]
async fn deleting_a_groups_current_min_and_max_recomputes_it_during_a_build() {
    let flavour = Flavour::MinMax;
    let rows = [(1, 0, 5), (2, 0, 20), (3, 0, 1), (4, 0, 10)];

    let (mut d, plan) = start_build(flavour, &rows).await;
    d.chunk(&plan, None, "3").await;
    d.merge_all(&plan).await;
    assert_eq!(min_max(&d, 0).await, mm(1, 20, 3));
    d.chunk(&plan, Some("3"), "4").await;
    let user = d.user().await;
    user.batch_execute("delete from public.src where id in (2, 3)")
        .await
        .expect("delete the extremes");
    let batch = d.seal().await;
    d.drain(batch, "page").await;
    assert_eq!(
        min_max(&d, 0).await,
        mm(5, 10, 1),
        "the page recomputed the group from its entries, the chunk's key 4 among them"
    );
    let merged = merge_every(&d, &plan).await;
    assert_eq!((merged.folded, merged.recomputed), (1, 0));
    assert_eq!(min_max(&d, 0).await, mm(5, 10, 2));
    assert_oracle(&mut d, &plan, flavour).await;

    let (mut d, plan) = start_build(flavour, &rows).await;
    d.chunk(&plan, None, "4").await;
    d.merge_all(&plan).await;
    assert_eq!(min_max(&d, 0).await, mm(1, 20, 4));
    write_uncaptured(&d, "delete from public.src where id in (2, 3)").await;
    // The keys are gone from the source, so a chunk of the range leaves
    // their entries alone: the rebuild's sweep is what retires them.
    let start_xid: String = d
        .ctl
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("a start xid")
        .get(0);
    let pool = d.pool().clone();
    let mut client = pool.get().await.expect("pool");
    let txn = client.transaction().await.expect("begin");
    let swept = build::sweep_batch(&txn, &plan, &start_xid, None, 100)
        .await
        .expect("sweep");
    txn.commit().await.expect("commit the sweep");
    assert_eq!(swept.rederived, 4);
    let merged = merge_every(&d, &plan).await;
    assert_eq!(
        (merged.claimed, merged.folded, merged.recomputed),
        (1, 0, 1),
        "values left the group: recomputed, not folded"
    );
    assert_eq!(min_max(&d, 0).await, mm(5, 10, 2));
    assert_oracle(&mut d, &plan, flavour).await;
}

/// A rebuild's sweep leaves a quarantined key alone, as a chunk does and as
/// the drain does (#625 F-A5): keys 2 and 3 are deleted past capture, and
/// key 2 is quarantined. The sweep re-derives keys 1 and 3 and retires key
/// 3, and key 2's entry stays as it was until `release_key` re-derives it.
#[tokio::test]
async fn a_sweep_leaves_a_quarantined_key_alone() {
    let flavour = Flavour::Sum;
    let (d, plan) = start_build(flavour, &[(1, 1, 1), (2, 1, 2), (3, 1, 3)]).await;
    d.chunk(&plan, None, "3").await;
    d.merge_all(&plan).await;
    write_uncaptured(&d, "delete from public.src where id in (2, 3)").await;
    d.ctl
        .batch_execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, 'public.src', '2', 'test' from transform_definitions \
             where source_table = 'public.src'",
        )
        .await
        .expect("quarantine key 2");
    let start_xid: String = d
        .ctl
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("a start xid")
        .get(0);
    let pool = d.pool().clone();
    let mut client = pool.get().await.expect("pool");
    let txn = client.transaction().await.expect("begin");
    let swept = build::sweep_batch(&txn, &plan, &start_xid, None, 100)
        .await
        .expect("sweep");
    txn.commit().await.expect("commit the sweep");
    assert_eq!((swept.scanned, swept.rederived), (3, 2));
    d.merge_all(&plan).await;
    assert_eq!(
        group(&d, 1).await,
        Some((Some("3".to_string()), 2)),
        "key 3 retired; quarantined key 2 still counted"
    );
}

/// A fold reads the entries' current values, not the values the chunk saw
/// (#625 F5): a chunk counts key 2 (100) into group 1, whose row already
/// holds key 1 (5), and a page then updates key 2 to 7 before the merge.
/// The page recomputes group 1 from its entries (key 2 at 7). The merge's
/// delta row is add-only, so it folds, reading key 2's entry as it is now:
/// a fold of the chunk's 100 would leave the group's `MAX` at 100 for good.
#[tokio::test]
async fn a_fold_reads_the_entry_a_page_changed_after_its_chunk() {
    let flavour = Flavour::MinMax;
    let (mut d, plan) = start_build(flavour, &[(1, 1, 5), (2, 1, 100)]).await;
    d.chunk(&plan, None, "1").await;
    d.merge_all(&plan).await;
    d.chunk(&plan, Some("1"), "2").await;
    let user = d.user().await;
    user.batch_execute("update public.src set v = 7 where id = 2")
        .await
        .expect("the page's update");
    let batch = d.seal().await;
    d.drain(batch, "page").await;
    assert_eq!(
        min_max(&d, 1).await,
        mm(5, 7, 1),
        "the page's recompute; the chunk's member is still owed"
    );
    let merged = merge_every(&d, &plan).await;
    assert_eq!((merged.folded, merged.recomputed), (1, 0));
    assert_eq!(min_max(&d, 1).await, mm(5, 7, 2));
    assert_oracle(&mut d, &plan, flavour).await;
}

/// F1's review note (B5): a page can bring a group's accumulators to 0 while
/// the group still has live entries, because a delta it is owed is pending.
/// The row is deleted, and the merge that owes it creates it again: that
/// row is recomputed from the group's entries, not folded from the delta's
/// keys, which no longer name a member of the group.
///
/// Key K′ (2, v 3) is counted into group 1 by a pending chunk delta. One
/// page deletes key 3 (group 1's only counted member), inserts key 1 (v 5)
/// and moves K′ to group 2: group 1's row goes to members 0 and is deleted,
/// with key 1 live in it. The merge's delta for group 1 names K′ only.
#[tokio::test]
async fn a_group_deleted_with_live_entries_is_recomputed_by_the_merge() {
    // Not `Flavour::Composed`: its `SUM(v + 3)` keeps the row at a sum of 2,
    // and it recomputes every group anyway.
    for flavour in [Flavour::MinMax, Flavour::Bool] {
        let (mut d, plan) = start_build(flavour, &[(2, 1, 3), (3, 1, 1)]).await;
        d.chunk(&plan, Some("2"), "3").await;
        d.merge_all(&plan).await;
        d.chunk(&plan, None, "2").await;
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
        d.drain(batch, "page").await;
        let group_1 = d.rows("select g from public.agg where g = 1").await;
        assert_eq!(
            group_1,
            Vec::<String>::new(),
            "{flavour:?}: the page deleted group 1 at all zeros, with key 1 live in it"
        );
        let merged = merge_every(&d, &plan).await;
        assert_eq!(
            (merged.claimed, merged.folded, merged.recomputed),
            (1, 0, 1),
            "{flavour:?}"
        );
        assert_oracle(&mut d, &plan, flavour).await;
    }
}

/// The other channel's B5 case (#625 F5): the merger itself empties a group
/// whose entries are live. Keys K′ (2) and K (3) are counted into group 1 by
/// two chunks' pending deltas, and a page moves K′ to group 2, taking group
/// 1 to -1. Merging K′'s delta alone takes it to 0, and the merger deletes
/// it with K live in it. Merging K's delta then creates the row again, and
/// recomputes it.
#[tokio::test]
async fn a_group_the_merger_empties_with_live_entries_is_recomputed_when_recreated() {
    let flavour = Flavour::MinMax;
    let (mut d, plan) = start_build(flavour, &[(2, 1, 3), (3, 1, 9)]).await;
    d.chunk(&plan, None, "2").await;
    d.chunk(&plan, Some("2"), "3").await;
    let user = d.user().await;
    user.batch_execute("update public.src set g = 2 where id = 2")
        .await
        .expect("move K′");
    let batch = d.seal().await;
    d.drain(batch, "page").await;
    assert_eq!(min_max(&d, 1).await, mm(9, 9, -1));
    let merged = d.merge(&plan, 1).await;
    assert_eq!((merged.claimed, merged.deleted), (1, 1));
    assert_eq!(min_max(&d, 1).await, None);
    let merged = d.merge(&plan, 1).await;
    assert_eq!(
        (merged.claimed, merged.written, merged.recomputed),
        (1, 1, 1)
    );
    assert_eq!(min_max(&d, 1).await, mm(9, 9, 1));
    assert_oracle(&mut d, &plan, flavour).await;
}

/// A fresh build's merges fold (#625 F5, #698): the first merge of a group
/// creates its row and recomputes it, and every later one only adds
/// entries to it, so it folds them without re-reading the group. A float
/// `SUM` or a composed field doesn't fold, so every merge recomputes.
#[tokio::test]
async fn a_fresh_builds_later_merges_fold() {
    for (flavour, folds) in [
        (Flavour::MinMax, true),
        (Flavour::Bool, true),
        (Flavour::Composed, false),
    ] {
        let (mut d, plan) = start_build_with(
            flavour,
            "insert into public.src select i, i % 3, i % 11 from generate_series(1, 60) i;",
        )
        .await;
        d.chunk(&plan, None, "30").await;
        let first = merge_every(&d, &plan).await;
        assert_eq!((first.folded, first.recomputed), (0, 3), "{flavour:?}");
        d.chunk(&plan, Some("30"), "60").await;
        let second = merge_every(&d, &plan).await;
        assert_eq!(
            (second.folded, second.recomputed),
            if folds { (3, 0) } else { (0, 3) },
            "{flavour:?}"
        );
        assert_oracle(&mut d, &plan, flavour).await;
    }
}

/// A fold over a float `MIN`/`MAX` and the `NULL` group (#625 F5): the
/// first merge creates each group's row and recomputes it, and the second
/// only adds entries, so it folds every group, the `NULL` one included.
/// The fold is exact because `min`/`max` order floats by Postgres's own
/// order (`NaN` above `Infinity`), the same order a full recompute uses:
/// `-Infinity` and `NaN` arriving in the second merge replace the stored
/// extremes, and an `Infinity` arriving after a `NaN` doesn't. The `NULL`
/// group's delta rows meet their keys by the group columns' own equality
/// (`NULL` matching `NULL`); a plain `=` would leave its keys out, and the
/// fold would skip the group as unchanged.
#[tokio::test]
async fn a_fold_takes_float_extremes_and_the_null_group() {
    const DEFINITION: &str = "TRANSFORM agg FROM public.src GROUP BY g \
         SELECT MIN(f) AS lo, MAX(f) AS hi, COUNT(*) AS n";
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, f float8); \
         insert into public.src values \
             (1, null, 1.5), (2, null, 2), (3, 1, 0.5), (4, 1, 'Infinity'), (5, 2, 'NaN'), \
             (6, null, 'NaN'), (7, null, '-Infinity'), (8, 1, '-Infinity'), (9, 1, 3), \
             (10, 2, 'Infinity'), (11, 2, -1);",
        &[
            ("id", ValueType::Numeric),
            ("g", ValueType::Numeric),
            ("f", ValueType::Float(trellis::FloatWidth::Float8)),
        ],
        &[DEFINITION],
        &["public.src"],
    )
    .await;
    d.ctl
        .batch_execute("truncate public.agg__ledger, public.agg__deltas, public.agg")
        .await
        .expect("empty what the build writes");
    let plan = d.build_plan("agg").await;
    d.chunk(&plan, None, "5").await;
    let first = merge_every(&d, &plan).await;
    assert_eq!((first.folded, first.recomputed), (0, 3));
    d.chunk(&plan, Some("5"), "11").await;
    let second = merge_every(&d, &plan).await;
    assert_eq!(
        (second.folded, second.recomputed),
        (3, 0),
        "every group, the NULL one included, only gained entries"
    );
    d.settle().await;
    let actual = d
        .rows("select g, lo, hi, n from public.agg order by g")
        .await;
    assert_eq!(
        actual,
        d.rows("select g, min(f), max(f), count(*) from public.src group by g order by g")
            .await
    );
    assert_eq!(
        actual,
        [
            "(1,-Infinity,Infinity,4)",
            "(2,-1,NaN,3)",
            "(,-Infinity,NaN,4)"
        ]
    );
}

/// A fold over expression arguments of mixed types (#625 F5): the stored
/// field (the target column's type) and the entries' aggregate (the
/// ledger's contribution column's) meet in one `min`/`max`/`bool_or`, whose
/// result is written back with the value a full recompute would write.
#[tokio::test]
async fn a_fold_takes_expression_arguments() {
    const DEFINITION: &str = "TRANSFORM agg FROM public.src GROUP BY g \
         SELECT MAX(v + w) AS hi, MIN(w + w) AS lo, BOOL_OR(v > 1) AS any_big, \
                MIN(i + 1) AS small, COUNT(*) AS n";
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, v numeric, w bigint, \
                                  i integer); \
         insert into public.src values \
             (1, 1, 0.5, 7, 3), (2, 2, 1.25, -3, 2147483646), (3, 1, null, null, null), \
             (4, 1, 2.50, 4000000000000000000, -2147483648), (5, 2, -1, 9, 0), \
             (6, 1, 1, -4000000000000000000, 5);",
        &[
            ("id", ValueType::Numeric),
            ("g", ValueType::Numeric),
            ("v", ValueType::Numeric),
            ("w", ValueType::Numeric),
            ("i", ValueType::Numeric),
        ],
        &[DEFINITION],
        &["public.src"],
    )
    .await;
    d.ctl
        .batch_execute("truncate public.agg__ledger, public.agg__deltas, public.agg")
        .await
        .expect("empty what the build writes");
    let plan = d.build_plan("agg").await;
    d.chunk(&plan, None, "3").await;
    let first = merge_every(&d, &plan).await;
    assert_eq!((first.folded, first.recomputed), (0, 2));
    d.chunk(&plan, Some("3"), "6").await;
    let second = merge_every(&d, &plan).await;
    assert_eq!((second.folded, second.recomputed), (2, 0));
    d.settle().await;
    assert_eq!(
        d.rows("select g, hi, lo, any_big, small, n from public.agg order by g")
            .await,
        d.rows(
            "select g, max(v + w), min(w + w), bool_or(v > 1), min(i::bigint + 1), count(*) \
             from public.src group by g order by g"
        )
        .await
    );
}

/// Expression arguments are rendered to SQL once (`defs::oracle`), and
/// computed twice: a chunk over the source table, a page over its change's
/// image cast to the source's row type (#623 D4). The two agree to the text
/// of every contribution (#625 F5): a chunk builds keys 1–8, a page applies
/// the same rows inserted again as keys 1001–1008, and each pair of entries
/// holds the same contributions. The values take in `NULL`s, negatives,
/// scales that differ (`1.50`), a large `bigint` sum and a boolean argument.
#[tokio::test]
async fn a_chunk_and_a_page_compute_expression_arguments_alike() {
    const DEFINITION: &str = "TRANSFORM agg FROM public.src GROUP BY g \
         SELECT SUM(v + v + 1) AS a, MAX(v + w) AS b, MIN(w + w) AS c, \
                BOOL_OR(v > 1) AS d, AVG(v + 0.5) AS e, COUNT(w + 1) AS f";
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, v numeric, w bigint); \
         insert into public.src values \
             (1, 1, 1.50, 7), (2, 1, -2.25, null), (3, 2, null, -5), \
             (4, 2, 0, 3000000000), (5, 3, 123456789.123456789, 4000000000000000000), \
             (6, 3, 1, 1), (7, 1, 2.000, -4000000000000000000), (8, 2, -0.5, 0);",
        &[
            ("id", ValueType::Numeric),
            ("g", ValueType::Numeric),
            ("v", ValueType::Numeric),
            ("w", ValueType::Numeric),
        ],
        &[DEFINITION],
        &["public.src"],
    )
    .await;
    d.ctl
        .batch_execute("truncate public.agg__ledger, public.agg__deltas, public.agg")
        .await
        .expect("empty what the build writes");
    let plan = d.build_plan("agg").await;
    let built = d.chunk(&plan, None, "8").await;
    assert_eq!(built.keys, 8);
    let user = d.user().await;
    user.batch_execute(
        "insert into public.src select id + 1000, g, v, w from public.src where id <= 8",
    )
    .await
    .expect("the twins");
    let batch = d.seal().await;
    d.drain(batch, "page").await;

    let args: Vec<String> = d
        .ctl
        .query(
            "select column_name::text from information_schema.columns \
             where table_schema = 'public' and table_name = 'agg__ledger' \
               and column_name like '\\_\\_arg%' order by column_name",
            &[],
        )
        .await
        .expect("the contribution columns")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(args.len(), 6, "{args:?}");
    let contributions = args
        .iter()
        .map(|c| format!("l.{c}::text"))
        .collect::<Vec<_>>()
        .join(", ");
    let pairs: Vec<(String, String)> = d
        .ctl
        .query(
            &format!(
                "select array[{contributions}]::text, \
                        (select array[{contributions}]::text from public.agg__ledger l \
                         where l.__from_key = ((c.__from_key::int) + 1000)::text) \
                 from public.agg__ledger c cross join lateral (select c.*) l \
                 where c.__from_key::int <= 8 order by c.__from_key::int"
            ),
            &[],
        )
        .await
        .expect("read the entry pairs")
        .iter()
        .map(|row| {
            (
                row.get(0),
                row.get::<_, Option<String>>(1).unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(pairs.len(), 8);
    for (chunk, page) in &pairs {
        assert_eq!(chunk, page, "a chunk's contributions against a page's");
    }
    d.merge_all(&plan).await;
    d.settle().await;
    assert_eq!(
        d.rows("select g, a, b, c, d, e, f from public.agg order by g")
            .await,
        d.rows(
            "select g, sum(v + v + 1), max(v + w), min(w + w), bool_or(v > 1), \
                    avg(v + 0.5), count(w + 1) \
             from public.src group by g order by g"
        )
        .await
    );
}

// --------------------------------------------------------------- deadlocks

/// Mergers and pages on the same groups never deadlock: a merger claims
/// delta rows without waiting (`skip locked`) and upserts its groups in group
/// order, and a page locks its entries and then its groups in the same
/// order. A second merger of the target takes another partition (#717), so
/// it writes other groups, rather than queue on the first's.
///
/// First forced: merger A is frozen holding its partition's groups, merger
/// B merges another partition beside it, and a page over every group queues
/// on A's groups. Then a free-running round: chunk runners, two mergers and
/// pages at once, with the source moving under them.
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
    let merger_b = d.merge(&plan, 1_000).await;
    assert!(
        !merger_b.skipped && merger_b.claimed > 0,
        "B merges another partition beside A: {merger_b:?}"
    );
    let user = d.user().await;
    user.batch_execute("update public.src set v = v + 1 where id <= 200")
        .await
        .expect("touch every group");
    let batch = d.seal().await;
    let page = d.drain_frozen(batch, "page", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut merger_a, PausePoint::AfterGroupUpsert).await;
    let merger_a = merger_a.finish().await;
    assert!(merger_a.claimed > 0, "{merger_a:?}");
    assert_ne!(merger_a.partition, merger_b.partition);
    page.finish().await;
    assert!(d.merge_all(&plan).await > 0, "the rest, after A");

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

// ---------------------------------------------------------------- mergers

/// The merge partitions the target's delta rows are in (#717), in order.
async fn partitions(d: &Driver) -> Vec<i16> {
    d.ctl
        .query(
            "select distinct __part from public.agg__deltas order by 1",
            &[],
        )
        .await
        .expect("read the delta rows' partitions")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// Mergers of different partitions merge side by side (#717): merger A is
/// frozen after its upsert, holding its partition's lock and its groups'
/// rows, and further mergers take every other partition, one each, without
/// waiting on A. A merge that waited on A's group rows or lock would hang
/// the test rather than pass it, and the timeout turns that into a failure.
/// Once only A's partition has rows, a merger skips the target. A commits,
/// and the target equals the oracle.
#[tokio::test]
async fn mergers_of_different_partitions_merge_side_by_side() {
    let flavour = Flavour::Sum;
    let (mut d, plan) = start_build_with(
        flavour,
        "insert into public.src select i, i % 20, i from generate_series(1, 200) i;",
    )
    .await;
    d.chunk(&plan, None, "100").await;
    d.chunk(&plan, Some("100"), "200").await;
    let before = partitions(&d).await;
    assert!(
        before.len() > 2,
        "20 groups spread over partitions: {before:?}"
    );

    let mut a = d
        .merge_frozen(&plan, 1_000, &[(PausePoint::AfterGroupUpsert, TARGET)])
        .await;
    a.reached(PausePoint::AfterGroupUpsert).await;
    let mut merged = Vec::new();
    loop {
        let outcome = tokio::time::timeout(Duration::from_secs(30), d.merge(&plan, 1_000))
            .await
            .expect("a merger never waits on another partition's merger");
        if outcome.skipped {
            assert_eq!(outcome.claimed, 0, "{outcome:?}");
            break;
        }
        assert!(outcome.claimed > 0, "{outcome:?}");
        merged.push(outcome.partition.expect("a merge names its partition"));
    }
    let remaining = partitions(&d).await;
    assert_eq!(
        remaining.len(),
        1,
        "only A's partition is left: {remaining:?}"
    );
    let a_partition = remaining[0];
    merged.sort_unstable();
    let mut others = before.clone();
    others.retain(|p| *p != a_partition);
    assert_eq!(
        merged, others,
        "each other partition merged once, apart from A's"
    );

    d.release(&mut a, PausePoint::AfterGroupUpsert).await;
    let a = a.finish().await;
    assert_eq!(a.partition, Some(a_partition));
    assert_eq!(deltas(&d).await, 0);
    assert_oracle(&mut d, &plan, flavour).await;
}

/// Two mergers never hold one partition (#717, as F2b's one merger per
/// target): with every delta row in one group, so one partition, a second
/// merger returns at once, skipped, with nothing claimed, instead of waiting
/// on the first's group row; once the first commits, the next merger takes
/// what is left.
#[tokio::test]
async fn a_second_merger_skips_a_partition_being_merged() {
    let flavour = Flavour::Sum;
    let (mut d, plan) = start_build_with(
        flavour,
        "insert into public.src select i, 1, i from generate_series(1, 200) i;",
    )
    .await;
    d.chunk(&plan, None, "100").await;
    d.chunk(&plan, Some("100"), "200").await;
    assert_eq!(partitions(&d).await.len(), 1);
    let mut first = d
        .merge_frozen(&plan, 1, &[(PausePoint::AfterGroupUpsert, TARGET)])
        .await;
    first.reached(PausePoint::AfterGroupUpsert).await;

    let second = tokio::time::timeout(Duration::from_secs(30), d.merge(&plan, 1_000))
        .await
        .expect("the second merger returns without waiting on the first");
    assert_eq!(
        second,
        MergeOutcome {
            partition: None,
            claimed: 0,
            written: 0,
            deleted: 0,
            folded: 0,
            recomputed: 0,
            skipped: true,
        }
    );
    assert_eq!(deltas(&d).await, 2, "nothing is merged until A commits");

    d.release(&mut first, PausePoint::AfterGroupUpsert).await;
    let first = first.finish().await;
    assert_eq!((first.claimed, first.skipped), (1, false));
    let after = d.merge(&plan, 1_000).await;
    assert_eq!((after.claimed, after.skipped), (1, false));
    assert_eq!(after.partition, first.partition);
    assert_oracle(&mut d, &plan, flavour).await;
}

/// The merge statement's plan doesn't hang on the delta table's statistics
/// (#625 F2b). A queue's churn lets an analyze record `reltuples = 0` for a
/// table that has rows by the time a merger claims them; the planner then
/// expects one claimed row and joins the upsert's result back to the group
/// sums in an inner nested loop, quadratic in the batch (the F2 profile's
/// 10M merges: 1.8 s against 25 ms for 5,000 rows). Under the merger's plan
/// settings the join is hashed and the claim reads the claim key's index.
#[tokio::test]
async fn the_merge_plan_survives_empty_statistics() {
    let (d, plan) = start_build_with(
        Flavour::Sum,
        "insert into public.src select i, i, i from generate_series(1, 300) i;",
    )
    .await;
    d.ctl
        .batch_execute(
            "insert into public.agg__deltas (g, __dm, __dc0, __ds0) \
                 select i, 1, 1, 1 from generate_series(1, 5000) i; \
             delete from public.agg__deltas; \
             analyze public.agg__deltas",
        )
        .await
        .expect("leave the delta table statistics of an emptied queue");
    d.chunk(&plan, None, "300").await;
    assert_eq!(deltas(&d).await, 300);

    let mut client = d.db.pool.get().await.expect("pool");
    let txn = client.transaction().await.expect("begin");
    let partition: i16 = txn
        .query_one(
            "select __part from public.agg__deltas group by 1 order by count(*) desc limit 1",
            &[],
        )
        .await
        .expect("the fullest partition")
        .get(0);
    let explained = build::explain_merge(&txn, &plan, 5_000, partition)
        .await
        .expect("explain the merge");
    let inner_nested_loops: Vec<&str> = explained
        .lines()
        .filter(|line| line.contains("Nested Loop") && !line.contains("Left Join"))
        .collect();
    assert_eq!(
        inner_nested_loops,
        Vec::<&str>::new(),
        "no inner nested loop:\n{explained}"
    );
    assert!(
        explained.contains("Hash Join"),
        "the upsert joins the sums by hash:\n{explained}"
    );
    assert!(
        explained.contains("Index Scan using agg__deltas___part___seq_idx"),
        "the claim reads the claim key's index:\n{explained}"
    );
}

/// A build's Re-derive statements reach the ledger through the key's
/// index, and the source through its primary key, while the ledger's
/// statistics lag its growth (#778): never analyzed (`empty`), or analyzed
/// while it held a handful of entries (`stale`), and then grown by a large
/// insert, as a build fills it. Left to the planner, the chunk's entry
/// rewrite joined its keys to a scan of the whole ledger, as the 1-1
/// chunk's did before `CHUNK_PLAN_SETTINGS` (#625 F8a).
#[tokio::test]
async fn the_rederive_statements_read_the_ledger_by_key() {
    let mut unbounded = Vec::new();
    for stats in ["empty", "stale"] {
        let (d, plan) = start_build_with(
            Flavour::Sum,
            "insert into public.src select i, i % 400, i from generate_series(1, 400000) i;",
        )
        .await;
        let load = |from: i32, to: i32| {
            format!(
                "insert into public.agg__ledger (__from_key, g, __arg0, __member) \
                 select i::text, i % 400, i, true from generate_series({from}, {to}) i"
            )
        };
        d.ctl
            .batch_execute("alter table public.agg__ledger set (autovacuum_enabled = false)")
            .await
            .expect("keep autoanalyze off the ledger");
        if stats == "stale" {
            d.ctl
                .batch_execute(&format!("{}; analyze public.agg__ledger", load(1, 100)))
                .await
                .expect("load and analyze a small ledger");
        }
        let from = if stats == "stale" { 101 } else { 1 };
        d.ctl
            .batch_execute(&load(from, 400_000))
            .await
            .expect("grow the ledger");
        let keys: Vec<String> = (10_001..=15_000).map(|i: i32| i.to_string()).collect();
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        let mut client = d.db.pool.get().await.expect("pool");
        let txn = client.transaction().await.expect("begin");
        let plans = build::explain_rederive(&txn, &plan, Some("10000"), "15000", &keys)
            .await
            .expect("explain the Re-derive statements");
        txn.rollback().await.expect("roll back");
        for (statement, explained) in plans {
            let bounded = !explained.lines().any(|line| {
                line.contains("Seq Scan")
                    || (line.contains("Scan")
                        && line.contains(" on agg__ledger ")
                        && estimated_rows(line) > keys.len())
            }) && explained
                .lines()
                .any(|line| line.contains("Index Cond: (__from_key = "));
            if !bounded {
                unbounded.push(format!("{stats} statistics, {statement}:\n{explained}"));
            }
        }
    }
    assert!(
        unbounded.is_empty(),
        "every read of the ledger is by the keys:\n{}",
        unbounded.join("\n")
    );
}

/// The Re-derive statements' plan settings end with each statement (#778):
/// the rest of the chunk's transaction plans as usual. The second chunk over
/// the range finds its entries, so it takes their lock as well.
#[tokio::test]
async fn the_entry_plan_settings_end_with_each_statement() {
    let (d, plan) = start_build_with(
        Flavour::Sum,
        "insert into public.src select i, i % 20, i from generate_series(1, 200) i;",
    )
    .await;
    d.chunk(&plan, None, "200").await;
    let mut client = d.db.pool.get().await.expect("pool");
    let txn = client.transaction().await.expect("begin");
    let outcome = build::run_chunk(&txn, &plan, None, "200")
        .await
        .expect("chunk");
    assert_eq!(outcome.keys, 200, "{outcome:?}");
    let seqscan: String = txn
        .query_one("select current_setting('enable_seqscan')", &[])
        .await
        .expect("read the plan setting")
        .get(0);
    assert_eq!(
        seqscan, "on",
        "the chunk's transaction plans as usual after its statements"
    );
    txn.commit().await.expect("commit");
}

/// The row estimate of an `explain` line's node: its `rows=`.
fn estimated_rows(line: &str) -> usize {
    line.split("rows=")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .and_then(|rows| rows.parse().ok())
        .unwrap_or(0)
}

/// The merge statement's plan settings end with the statement (#625 F2b):
/// the rest of the merger's transaction, the empty-group delete and the
/// seam's statements, plans as usual.
#[tokio::test]
async fn the_merge_plan_settings_end_with_the_merge_statement() {
    let (d, plan) = start_build_with(
        Flavour::Sum,
        "insert into public.src select i, i % 20, i from generate_series(1, 200) i;",
    )
    .await;
    d.chunk(&plan, None, "200").await;
    let mut client = d.db.pool.get().await.expect("pool");
    let txn = client.transaction().await.expect("begin");
    let outcome = build::merge_deltas(&txn, &plan, 1_000)
        .await
        .expect("merge");
    assert!(outcome.claimed > 0 && !outcome.skipped, "{outcome:?}");
    let row = txn
        .query_one(
            "select current_setting('enable_nestloop'), current_setting('enable_seqscan')",
            &[],
        )
        .await
        .expect("read the plan settings");
    assert_eq!(
        (row.get::<_, String>(0), row.get::<_, String>(1)),
        ("on".to_string(), "on".to_string()),
        "the merger's transaction plans as usual after its merge statement"
    );
    txn.commit().await.expect("commit");
}

/// The delta rows' claim keys, in the heap's order.
async fn seqs(d: &Driver) -> Vec<i64> {
    d.ctl
        .query("select __seq from public.agg__deltas order by ctid", &[])
        .await
        .expect("read the delta rows")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// The merger claims a partition's oldest delta rows first, in append
/// order, through the claim key's index (#625 F2b, #717), not in the heap's
/// physical order. A delta row rewritten in place keeps its claim key but
/// moves to the end of the heap, so a physical-order claim would take the
/// newer rows first. One group, so every row is in one partition.
#[tokio::test]
async fn the_merger_claims_the_oldest_delta_rows_first() {
    let flavour = Flavour::Sum;
    let (mut d, plan) = start_build(flavour, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]).await;
    d.chunk(&plan, None, "1").await;
    d.chunk(&plan, Some("1"), "2").await;
    d.chunk(&plan, Some("2"), "3").await;
    let appended = seqs(&d).await;
    assert_eq!(appended.len(), 3);
    d.ctl
        .batch_execute(&format!(
            "update public.agg__deltas set __dm = __dm where __seq = {}",
            appended[0]
        ))
        .await
        .expect("move the oldest delta row to the end of the heap");
    assert_eq!(
        seqs(&d).await,
        [appended[1], appended[2], appended[0]],
        "the heap's order"
    );

    for left in [&appended[1..], &appended[2..], &[]] {
        assert_eq!(d.merge(&plan, 1).await.claimed, 1);
        let mut remaining = seqs(&d).await;
        remaining.sort_unstable();
        assert_eq!(remaining, left, "the oldest row went first");
    }
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
    for flavour in Flavour::EVERY {
        let rows = [(1, 1, 1), (2, 1, 2), (3, 2, 3), (4, 2, 4), (5, 1, 5)];
        let d = Driver::start(
            &format!("{} {}", flavour.create_source(), seed(&rows)),
            &flavour.columns(),
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

/// A chunk takes its entry lock before it reads (ADR-0002 I1 for the build,
/// #625 F3's `chunk_without_entry_lock` plant). Key 2 already has an entry
/// (an Apply counted it before its chunk ran), and a page applying a later
/// change to it is frozen after its entry lock, before it writes. The chunk
/// over key 2 queues behind it in its entry lock, and its read sees the
/// page's commit. Read before the lock, it would see the entry as it was
/// before the page, then wait on the page's row lock in its write and move
/// key 2 from that stale entry, counting the page's change twice. (A page
/// frozen after its write would hold the chunk anyway: the chunk's
/// placeholder insert waits on the entry's in-progress update.) A chunk
/// that gives up at its short lock timeout runs again.
#[tokio::test]
async fn a_chunk_reads_an_entry_only_once_the_page_holding_it_commits() {
    let flavour = Flavour::Sum;
    let (mut d, plan) = start_build(flavour, &[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
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
    let chunk = d.chunk_frozen(&plan, None, "3", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut page, PausePoint::AfterEntryLock).await;
    page.finish().await;
    match chunk.finish_result().await {
        Ok(outcome) => assert_eq!(outcome.keys, 3),
        Err(err) if trellis::locks::is_lock_not_available(&err) => {
            assert_eq!(d.chunk(&plan, None, "3").await.keys, 3);
        }
        Err(err) => panic!("the chunk failed: {err}"),
    }
    assert_oracle(&mut d, &plan, flavour).await;
}

/// A page's Apply of a key with no entry writes the entry in its lock's
/// insert (#775), and that uncommitted entry is the key's lock: a chunk over
/// the key queues on it in its own placeholder insert, finds the entry once
/// the page commits, and reads after it (I1). Key 2 has no entry (the build
/// hasn't reached it); a page applying its update is frozen holding the
/// entry it inserted, and key 2 then moves to group 2. The chunk moves key 2
/// from the page's entry to the row it reads, and the move's own batch,
/// drained last, is visible in the chunk's basis and skipped. A chunk that
/// gives up at its short lock timeout runs again.
#[tokio::test]
async fn a_chunk_queues_on_an_entry_a_page_inserted_with_its_change() {
    for flavour in Flavour::EVERY {
        let (mut d, plan) = start_build(flavour, &[(1, 1, 1), (2, 1, 2), (3, 2, 3)]).await;
        let user = d.user().await;
        user.batch_execute("update public.src set v = v + 10 where id = 2")
            .await
            .expect("update key 2");
        let batch = d.seal().await;
        let mut page = d
            .drain_frozen(batch, "page", &[(PausePoint::AfterEntryLock, TARGET)])
            .await;
        let frozen = page.reached(PausePoint::AfterEntryLock).await;
        user.batch_execute("update public.src set g = 2, v = v + 100 where id = 2")
            .await
            .expect("move key 2");
        let moved = d.seal().await;
        let chunk = d.chunk_frozen(&plan, None, "3", &[]).await;
        d.wait_blocked_behind(frozen.backend_pid).await;
        d.release(&mut page, PausePoint::AfterEntryLock).await;
        page.finish().await;
        match chunk.finish_result().await {
            Ok(outcome) => assert_eq!(outcome.keys, 3),
            Err(err) if trellis::locks::is_lock_not_available(&err) => {
                assert_eq!(d.chunk(&plan, None, "3").await.keys, 3);
            }
            Err(err) => panic!("{flavour:?}: the chunk failed: {err}"),
        }
        d.drain(moved, "apply").await;
        assert_oracle(&mut d, &plan, flavour).await;
    }
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

// ------------------------------- an entry the GC collects mid-lock (#712)

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

/// A page whose entry lock loses a key to the GC gives up every lock it
/// holds before inserting that key's placeholder again, so a chunk that
/// took the placeholder in the meantime gets every lock it asks for. The
/// page (keys 1 and 2) is frozen after its placeholder insert, which found
/// key 2's tombstone. The GC collects it, and the chunk (keys 1 to 3)
/// inserts key 2's placeholder and is frozen there. The page locks key 1,
/// finds no entry for key 2, rolls back and retries, and its retry queues on
/// the chunk's placeholder; then the chunk locks its keys. Had the page kept
/// key 1's lock, the chunk would queue on it: a deadlock, or the chunk giving
/// up at its lock timeout.
#[tokio::test]
async fn a_page_losing_an_entry_to_the_gc_never_deadlocks_with_a_chunk() {
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

/// A chunk whose entry lock loses a key to the GC gives up with a transient
/// error, to be retried, rather than re-derive its range with that key
/// unlocked. The chunk (keys 1 to 3) is frozen after its placeholder
/// insert, which found key 2's tombstone, and the GC collects it.
#[tokio::test]
async fn a_chunk_losing_an_entry_to_the_gc_gives_up_transiently() {
    let flavour = Flavour::Sum;
    let (mut d, plan, batch) = start_retake(flavour).await;
    let mut chunk = d
        .chunk_frozen(&plan, None, "3", &[(PausePoint::AfterPlaceholders, TARGET)])
        .await;
    chunk.reached(PausePoint::AfterPlaceholders).await;
    assert_eq!(d.collect_tombstones().await, 1, "key 2's tombstone goes");
    d.release(&mut chunk, PausePoint::AfterPlaceholders).await;
    let err = chunk.finish_result().await.expect_err("key 2 has no entry");
    assert!(
        matches!(err, ApplyError::LedgerEntryCollected { .. }),
        "the entry was collected, not {err}"
    );
    assert_eq!(classify(&err), FailureClass::Transient);
    d.drain(batch, "page").await;
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
    eight_chunk_runners_under_writers(Flavour::Avg).await;
}

/// [`a_build_by_eight_chunk_runners_under_writers_equals_the_oracle`] for
/// `MIN`/`MAX` (#625 F5): every merge folds or recomputes its groups while
/// pages recompute theirs.
#[tokio::test]
async fn a_min_max_build_by_eight_chunk_runners_under_writers_equals_the_oracle() {
    eight_chunk_runners_under_writers(Flavour::MinMax).await;
}

async fn eight_chunk_runners_under_writers(flavour: Flavour) {
    const ROWS: i64 = 200_000;
    const CHUNK_ROWS: i64 = 10_000;
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
