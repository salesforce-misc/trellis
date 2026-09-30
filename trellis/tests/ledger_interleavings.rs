//! Deterministic interleavings of the drain on the real engine (#623 D1,
//! epic #556, ADR-0002 invariants I1 and I2).
//!
//! Each test drives one interleaving by hand through
//! `support/drain_driver.rs`: source writes go through trigger capture, the
//! test seals batches itself and drains each one on a worker it names,
//! freezing a worker at a pause point (`trellis::staging::interleave`) where
//! the scenario needs one. No test sleeps or polls for convergence (#297).
//! Every test ends by checking the target against a from-scratch oracle over
//! the source.
//!
//! The scenarios are experiment 2's from #558
//! (<https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484>),
//! the shapes of the issues the ledger supersedes (#389, #392, #494, #539,
//! #550), and `chunked_read_exact_point` (the per-chunk exact read point,
//! the user's Q1 on the D split). Each runs in the flavours the shape
//! applies to: a plain `SUM`/`COUNT` aggregate, the same with an `AVG` and a
//! `COUNT(x)` (#623 D3b), and a 1-1 target.
//!
//! A scenario that fails on today's engine is `#[ignore]`d naming the D part
//! that makes it pass; that part un-ignores it. The wrong value each one
//! shows today is in its doc comment.
//!
//! "Re-derive" below is an image-less `Recompute` for a key. The aggregate
//! flavour is on the ledger since #623 D3, where it re-reads the key's row
//! and snapshot in one statement and rewrites the key's entry
//! (`trellis::staging::ledger`), and the `MIN`/`MAX` flavour since D4
//! recomputes its written groups from the entries. The 1-1 flavour re-reads
//! the row (#344) until D6.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::staging::interleave::PausePoint;
use trellis::staging::{StagedWatermark, apply, claim};

const SRC: &str = "public.src";

#[derive(Clone, Copy, Debug)]
enum Flavour {
    Aggregate,
    /// `AVG` beside a `SUM` sharing its hidden count, and `COUNT(x)`: on the
    /// ledger since #623 D3b.
    AggregateAvg,
    /// A recompute-only aggregate (`MIN`/`MAX`), for #494's D4 variant.
    AggregateMinMax,
    OneToOne,
}

impl Flavour {
    fn definition(self) -> &'static str {
        match self {
            Flavour::Aggregate => {
                "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n"
            }
            Flavour::AggregateAvg => {
                "TRANSFORM agg FROM public.src GROUP BY g \
                 SELECT SUM(v) AS total, AVG(v) AS mean, COUNT(v) AS nv, COUNT(*) AS n"
            }
            Flavour::AggregateMinMax => {
                "TRANSFORM agg FROM public.src GROUP BY g \
                 SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n"
            }
            Flavour::OneToOne => "TRANSFORM one FROM public.src SELECT g AS g, v AS v",
        }
    }

    fn target(self) -> &'static str {
        match self {
            Flavour::Aggregate | Flavour::AggregateAvg | Flavour::AggregateMinMax => "public.agg",
            Flavour::OneToOne => "public.one",
        }
    }

    fn actual(self) -> &'static str {
        match self {
            Flavour::Aggregate => "select g, total, n from public.agg order by g",
            Flavour::AggregateAvg => "select g, total, mean, nv, n from public.agg order by g",
            Flavour::AggregateMinMax => "select g, lo, hi, n from public.agg order by g",
            Flavour::OneToOne => "select id, g, v from public.one order by id",
        }
    }

    fn expected(self) -> &'static str {
        match self {
            Flavour::Aggregate => {
                "select g, sum(v), count(*) from public.src group by g order by g"
            }
            Flavour::AggregateAvg => {
                "select g, sum(v), avg(v), count(v), count(*) from public.src group by g order by g"
            }
            Flavour::AggregateMinMax => {
                "select g, min(v), max(v), count(*) from public.src group by g order by g"
            }
            Flavour::OneToOne => "select id, g, v from public.src order by id",
        }
    }

    /// Whether the flavour's target is on the ledger.
    fn on_ledger(self) -> bool {
        matches!(
            self,
            Flavour::Aggregate | Flavour::AggregateAvg | Flavour::AggregateMinMax
        )
    }
}

/// `public.src (id, g, v)` seeded with `rows`, one live `flavour` target
/// over it, captured by triggers, drained to quiescence.
async fn start(flavour: Flavour, rows: &[(i32, i32, i32)]) -> Driver {
    let values: Vec<String> = rows
        .iter()
        .map(|(id, g, v)| format!("({id}, {g}, {v})"))
        .collect();
    let seed = if values.is_empty() {
        String::new()
    } else {
        format!("insert into public.src values {};", values.join(", "))
    };
    Driver::start(
        &format!(
            "create table public.src (id integer primary key, g integer, v numeric); \
             {seed}"
        ),
        &[
            ("id", ValueType::Numeric),
            ("g", ValueType::Numeric),
            ("v", ValueType::Numeric),
        ],
        &[flavour.definition()],
        &[SRC],
    )
    .await
}

/// Settles the pipeline and asserts the target equals the oracle.
async fn assert_oracle(d: &mut Driver, flavour: Flavour) {
    d.settle().await;
    let actual = d.rows(flavour.actual()).await;
    let expected = d.rows(flavour.expected()).await;
    assert_eq!(
        actual, expected,
        "{flavour:?} target (left) differs from the oracle (right)"
    );
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

// ------------------------------------------------------------- the harness

/// Every pause point a flavour has fires, in page order, and holds the page
/// until released: a page with a Re-derive of key 1 and a new key 3 in a
/// new group.
async fn pause_points_fire_in_page_order(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 5)]).await;
    d.stage_recomputes(SRC, &["1"]).await;
    write(&d, "insert into public.src values (3, 2, 7)").await;
    let batch = d.seal().await;
    let mut points = vec![
        PausePoint::AfterPlaceholders,
        PausePoint::AfterEntryLock,
        PausePoint::AfterRederiveRead,
        PausePoint::AfterGroupUpsert,
        PausePoint::BeforeCommit,
    ];
    if matches!(flavour, Flavour::OneToOne) {
        points.retain(|p| *p != PausePoint::AfterGroupUpsert);
    }
    if !flavour.on_ledger() {
        points.retain(|p| *p != PausePoint::AfterPlaceholders);
    }
    let armed: Vec<(PausePoint, &str)> = points.iter().map(|p| (*p, flavour.target())).collect();
    let mut drain = d.drain_frozen(batch, "a", &armed).await;
    let mut pids = Vec::new();
    for point in points {
        let reached = drain.reached(point).await;
        assert_eq!(reached.point, point);
        pids.push(reached.backend_pid);
        d.release(&mut drain, point).await;
    }
    pids.dedup();
    assert_eq!(pids.len(), 1, "every point is in one page transaction");
    drain.finish().await;
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn pause_points_fire_in_page_order_aggregate() {
    pause_points_fire_in_page_order(Flavour::Aggregate).await;
}

#[tokio::test]
async fn pause_points_fire_in_page_order_one_to_one() {
    pause_points_fire_in_page_order(Flavour::OneToOne).await;
}

// ---------------------------------------------------------------- exp 2, 2b

/// Exp 2 scenario 2 (#344): a Re-derive of key 1 is frozen after its read,
/// and a CDC change C for the same key, committed *before* that read, drains
/// on a second worker. The second worker must queue behind the first (I1),
/// then count C exactly once.
async fn exp2_2(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 5)]).await;
    write(&d, "update public.src set v = 20 where id = 1").await;
    let c_batch = d.seal().await;
    d.stage_recomputes(SRC, &["1"]).await;
    let r_batch = d.seal().await;

    let mut a = d
        .drain_frozen(
            r_batch,
            "a",
            &[(PausePoint::AfterRederiveRead, flavour.target())],
        )
        .await;
    let frozen = a.reached(PausePoint::AfterRederiveRead).await;
    let b = d.drain_frozen(c_batch, "b", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut a, PausePoint::AfterRederiveRead).await;
    a.finish().await;
    b.finish().await;
    assert_oracle(&mut d, flavour).await;
}

/// Exp 2 scenario 2b: as [`exp2_2`], but C commits *after* the frozen
/// Re-derive's read, so the read did not see it and C must apply.
async fn exp2_2b(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 5)]).await;
    d.stage_recomputes(SRC, &["1"]).await;
    let r_batch = d.seal().await;
    let mut a = d
        .drain_frozen(
            r_batch,
            "a",
            &[(PausePoint::AfterRederiveRead, flavour.target())],
        )
        .await;
    let frozen = a.reached(PausePoint::AfterRederiveRead).await;
    write(&d, "update public.src set v = 20 where id = 1").await;
    let c_batch = d.seal().await;
    let b = d.drain_frozen(c_batch, "b", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut a, PausePoint::AfterRederiveRead).await;
    a.finish().await;
    b.finish().await;
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn exp2_2_aggregate() {
    exp2_2(Flavour::Aggregate).await;
}

#[tokio::test]
async fn exp2_2_one_to_one() {
    exp2_2(Flavour::OneToOne).await;
}

#[tokio::test]
async fn exp2_2b_aggregate() {
    exp2_2b(Flavour::Aggregate).await;
}

#[tokio::test]
async fn exp2_2b_one_to_one() {
    exp2_2b(Flavour::OneToOne).await;
}

// ------------------------------------------------------------------ exp 2, 3

/// Exp 2 scenario 3 (#321): C moves key 1 from group 1 to group 2 and
/// commits; a Re-derive of keys 1 and 2 (both groups) drains before C's own
/// batch, which drains after. C must not be counted twice.
#[tokio::test]
async fn exp2_3_aggregate() {
    let flavour = Flavour::Aggregate;
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 5), (3, 2, 7)]).await;
    write(&d, "update public.src set g = 2, v = 20 where id = 1").await;
    let c_batch = d.seal().await;
    d.stage_recomputes(SRC, &["1", "2"]).await;
    let r_batch = d.seal().await;
    d.drain(r_batch, "a").await;
    d.drain(c_batch, "b").await;
    assert_oracle(&mut d, flavour).await;
}

// ------------------------------------------------------------------ exp 2, 4

/// Inserts `rounds` batches of 8 writers × 40 rows each into groups
/// `group_of(round, i)`, drains every batch with 8 workers at once, and
/// returns the `deadlock detected` reports the server logged meanwhile.
async fn concurrent_group_writes(
    d: &mut Driver,
    rounds: i32,
    group_of: impl Fn(i32, u64) -> i32,
) -> Vec<String> {
    let before = d.deadlocks_logged().len();
    let mut seed: u64 = 12345;
    for round in 0..rounds {
        let mut values = Vec::new();
        for writer in 0..8 {
            let mut rows = Vec::new();
            for i in 0..40 {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let id = writer * 100_000 + round * 100 + i;
                rows.push(format!(
                    "({id}, {}, {})",
                    group_of(round, seed >> 33),
                    (seed >> 20) % 100
                ));
            }
            values.push(format!("insert into public.src values {}", rows.join(", ")));
        }
        let mut writers = Vec::new();
        for sql in values {
            let user = d.user().await;
            writers.push(tokio::spawn(async move {
                user.batch_execute(&sql)
                    .await
                    .expect("insert a writer's rows");
            }));
        }
        for writer in writers {
            writer.await.expect("writer task");
        }
        let batch = d.seal().await;
        let mut workers = Vec::new();
        for w in 0..8 {
            workers.push(d.drain_share(batch, &format!("w{w}"), 8).await);
        }
        for worker in workers {
            worker.finish().await;
        }
    }
    d.deadlocks_logged().split_off(before)
}

/// Exp 2 scenario 4 (#389/#539): 8 workers × 30 batches of 320 rows over the
/// same 20 groups, every batch split across the 8 workers. The groups are
/// right and no worker deadlocks.
///
/// Before the ledger (#623 D3) it failed about 1 run in 8 with 8 copies in
/// parallel: one drain's bulk group `update` against another's new-group
/// `insert ... on conflict`, each waiting on the other's transaction. The
/// ledger path locks entries, then groups, each in one sorted statement.
#[tokio::test]
async fn exp2_4_aggregate() {
    let flavour = Flavour::Aggregate;
    let mut d = start(flavour, &[]).await;
    let deadlocks = concurrent_group_writes(&mut d, 30, |_, r| (r % 20) as i32 + 1).await;
    assert_oracle(&mut d, flavour).await;
    assert!(
        deadlocks.is_empty(),
        "deadlocks detected while draining:\n{}",
        deadlocks.join("\n--\n")
    );
}

// ------------------------------------------------------------------ exp 2, 5

/// Seeds key 1 in group a (1), key 5 in z (2) and key 6 in b (3).
const A_Z_B: &[(i32, i32, i32)] = &[(1, 1, 10), (5, 2, 50), (6, 3, 60)];

/// Exp 2 scenario 5 (#494): key 1 moves a → z (C1), a Re-derive of z's
/// member 5 reads z live with key 1 in it and is frozen, then key 1 moves
/// z → b (C2). C1 and C2 fold into one record, a → b, drained while the
/// Re-derive is still open. z must end without key 1.
async fn exp2_5(flavour: Flavour) {
    let mut d = start(flavour, A_Z_B).await;
    d.stage_recomputes(SRC, &["5"]).await;
    let r_batch = d.seal().await;
    write(&d, "update public.src set g = 2 where id = 1").await;
    let mut a = d
        .drain_frozen(
            r_batch,
            "a",
            &[(PausePoint::AfterRederiveRead, flavour.target())],
        )
        .await;
    a.reached(PausePoint::AfterRederiveRead).await;
    write(&d, "update public.src set g = 3 where id = 1").await;
    let c_batch = d.seal().await;
    // Whether or not the folded record names z (and so queues behind the
    // frozen Re-derive), the result doesn't depend on which commits first:
    // the Re-derive writes only z.
    let b = d.drain_frozen(c_batch, "b", &[]).await;
    d.release(&mut a, PausePoint::AfterRederiveRead).await;
    a.finish().await;
    b.finish().await;
    assert_oracle(&mut d, flavour).await;
}

/// Before the ledger (#623 D3): z kept key 1, `(2,60,2)` against the
/// oracle's `(2,50,1)`. The folded record named only a and b, so nothing
/// corrected the live group re-derive's count of key 1 in z (#494's shape).
/// On the ledger a Re-derive of key 5 writes only key 5's entry.
#[tokio::test]
async fn exp2_5_aggregate() {
    exp2_5(Flavour::Aggregate).await;
}

/// Before the ledger (#623 D4): `(2,10,50,2)` against the oracle's
/// `(2,50,50,1)`, for the same reason as [`exp2_5_aggregate`].
#[tokio::test]
async fn exp2_5_aggregate_min_max() {
    exp2_5(Flavour::AggregateMinMax).await;
}

/// Exp 2 scenario 5b: a → z (C1) and z → b (C2) in two batches, drained out
/// of order (C2's first).
async fn exp2_5b(flavour: Flavour) {
    let mut d = start(flavour, A_Z_B).await;
    write(&d, "update public.src set g = 2 where id = 1").await;
    let first = d.seal().await;
    write(&d, "update public.src set g = 3 where id = 1").await;
    let second = d.seal().await;
    d.drain(second, "a").await;
    d.drain(first, "b").await;
    assert_oracle(&mut d, flavour).await;
}

/// Before the ledger (#623 D3): z ended `(2,10,1)` against the oracle's
/// `(2,50,1)`. Applied first, C2 took key 1 out of z before C1 had put it
/// in, which dropped z's non-null count for `SUM(v)` to 0, so the total
/// reset to NULL although key 5 was still in z; C1's +10 then landed on the
/// reset total. On the ledger C2 moves key 1's entry from a to b, and C1,
/// older than the entry's `applied_lsn`, is skipped.
#[tokio::test]
async fn exp2_5b_aggregate() {
    exp2_5b(Flavour::Aggregate).await;
}

#[tokio::test]
async fn exp2_5b_one_to_one() {
    exp2_5b(Flavour::OneToOne).await;
}

// ------------------------------------------------------------------ exp 2, 9

/// Two changes to key 1, each sealed into its own batch, drained in the
/// order `drain_second_first` says.
async fn same_key_two_batches(flavour: Flavour, first: &str, second: &str, second_first: bool) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    write(&d, first).await;
    let b1 = d.seal().await;
    write(&d, second).await;
    let b2 = d.seal().await;
    if second_first {
        d.drain(b2, "a").await;
        d.drain(b1, "b").await;
    } else {
        d.drain(b1, "a").await;
        d.drain(b2, "b").await;
    }
    assert_oracle(&mut d, flavour).await;
}

/// Exp 2 scenario 9: the delete of key 1 drains, then its older update. The
/// row must stay deleted (a tombstone, I4).
async fn exp2_9(flavour: Flavour) {
    same_key_two_batches(
        flavour,
        "update public.src set v = 15 where id = 1",
        "delete from public.src where id = 1",
        true,
    )
    .await;
}

/// Exp 2 scenario 9b: the delete of key 1 drains, then its newer re-insert.
async fn exp2_9b(flavour: Flavour) {
    same_key_two_batches(
        flavour,
        "delete from public.src where id = 1",
        "insert into public.src values (1, 1, 30)",
        false,
    )
    .await;
}

/// Exp 2 scenario 9c: two updates of key 1 drain out of order.
async fn exp2_9c(flavour: Flavour) {
    same_key_two_batches(
        flavour,
        "update public.src set v = 15 where id = 1",
        "update public.src set v = 17 where id = 1",
        true,
    )
    .await;
}

/// Exp 2 scenario 9d: two updates of key 1 drain in order.
async fn exp2_9d(flavour: Flavour) {
    same_key_two_batches(
        flavour,
        "update public.src set v = 15 where id = 1",
        "update public.src set v = 17 where id = 1",
        false,
    )
    .await;
}

#[tokio::test]
async fn exp2_9_aggregate() {
    exp2_9(Flavour::Aggregate).await;
}

#[tokio::test]
async fn exp2_9_one_to_one() {
    exp2_9(Flavour::OneToOne).await;
}

#[tokio::test]
async fn exp2_9b_aggregate() {
    exp2_9b(Flavour::Aggregate).await;
}

#[tokio::test]
async fn exp2_9b_one_to_one() {
    exp2_9b(Flavour::OneToOne).await;
}

#[tokio::test]
async fn exp2_9c_aggregate() {
    exp2_9c(Flavour::Aggregate).await;
}

#[tokio::test]
async fn exp2_9c_one_to_one() {
    exp2_9c(Flavour::OneToOne).await;
}

#[tokio::test]
async fn exp2_9d_aggregate() {
    exp2_9d(Flavour::Aggregate).await;
}

#[tokio::test]
async fn exp2_9d_one_to_one() {
    exp2_9d(Flavour::OneToOne).await;
}

// ----------------------------------------------------------------- exp 2, 10

/// Exp 2 scenario 10: C (key 1's update) is in flight when a Re-derive of
/// key 1 reads, so the read doesn't see it; a later transaction commits
/// first, so C's id is below the read snapshot's `xmax` (in its `xip`). C
/// then commits and drains, and must apply.
async fn exp2_10(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    let writer = d.user().await;
    writer
        .batch_execute("begin; update public.src set v = 15 where id = 1")
        .await
        .expect("open C");
    write(&d, "update public.src set v = 21 where id = 2").await;
    d.stage_recomputes(SRC, &["1"]).await;
    let r_batch = d.seal().await;
    d.drain(r_batch, "a").await;
    writer.batch_execute("commit").await.expect("commit C");
    let c_batch = d.seal().await;
    d.drain(c_batch, "b").await;
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn exp2_10_aggregate() {
    exp2_10(Flavour::Aggregate).await;
}

#[tokio::test]
async fn exp2_10_one_to_one() {
    exp2_10(Flavour::OneToOne).await;
}

// ------------------------------------------------------ superseded issues

/// #389: two batches each create the same new group 7, above any horizon.
/// The first worker is frozen after its group upsert, with group 7's row
/// inserted and uncommitted; the second queues behind it. Both rows count.
#[tokio::test]
async fn issue_389_aggregate() {
    let flavour = Flavour::Aggregate;
    let mut d = start(flavour, &[(1, 1, 10)]).await;
    write(&d, "insert into public.src values (10, 7, 1)").await;
    let first = d.seal().await;
    write(&d, "insert into public.src values (11, 7, 2)").await;
    let second = d.seal().await;
    let mut a = d
        .drain_frozen(
            first,
            "a",
            &[(PausePoint::AfterGroupUpsert, flavour.target())],
        )
        .await;
    let frozen = a.reached(PausePoint::AfterGroupUpsert).await;
    let b = d.drain_frozen(second, "b", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut a, PausePoint::AfterGroupUpsert).await;
    a.finish().await;
    b.finish().await;
    assert_oracle(&mut d, flavour).await;
}

/// The CDC text-format image of `public.src`'s row `id`, as the go-live
/// enumeration attaches it to a `Recompute` (#392).
async fn src_image(d: &Driver, id: i32) -> String {
    d.ctl
        .query_one(
            "select jsonb_build_object('id', id::text, 'g', g::text, 'v', v::text)::text \
             from public.src where id = $1",
            &[&id],
        )
        .await
        .expect("read the row image")
        .get(0)
}

/// #392: a go-live `Recompute` for key 1 (carrying key 1's image, as the
/// enumeration stages it) drains after key 1 is deleted, and before the
/// delete's own batch. Group 1 must end without key 1.
async fn issue_392(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 5)]).await;
    let image = src_image(&d, 1).await;
    d.stage_recomputes_with_prior(SRC, &[("1", Some(image))])
        .await;
    let r_batch = d.seal().await;
    write(&d, "delete from public.src where id = 1").await;
    let c_batch = d.seal().await;
    d.drain(r_batch, "a").await;
    d.drain(c_batch, "b").await;
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn issue_392_aggregate() {
    issue_392(Flavour::Aggregate).await;
}

#[tokio::test]
async fn issue_392_one_to_one() {
    issue_392(Flavour::OneToOne).await;
}

/// #494, the issue body's repro: key 1 goes a → z (C1) at or below z's
/// Re-derive horizon, then z → b (C2) above it, and C1 and C2 fold into one
/// batch. The Re-derive (through z's member 5, sealed before C1 and drained
/// after it) commits with key 1 counted in z. z must end without key 1.
///
/// Before the ledger (#623 D3): z kept key 1, `(2,60,2)` against the
/// oracle's `(2,50,1)`.
#[tokio::test]
async fn issue_494_aggregate() {
    let flavour = Flavour::Aggregate;
    let mut d = start(flavour, A_Z_B).await;
    d.stage_recomputes(SRC, &["5"]).await;
    let r_batch = d.seal().await;
    write(&d, "update public.src set g = 2 where id = 1").await;
    d.drain(r_batch, "a").await;
    write(&d, "update public.src set g = 3 where id = 1").await;
    let c_batch = d.seal().await;
    d.drain(c_batch, "b").await;
    assert_oracle(&mut d, flavour).await;
}

/// #539: a burst of new groups, 8 workers at once: every one of 4 batches
/// of 320 rows creates 20 groups no batch before it touched. No worker
/// deadlocks.
///
/// Before the ledger (#623 D3) it failed about 1 run in 8 with 8 copies in
/// parallel: one drain's bulk group `update` against another's new-group
/// `insert ... on conflict`, each waiting on the other's transaction. The
/// ledger path locks entries, then groups, each in one sorted statement.
#[tokio::test]
async fn issue_539_aggregate() {
    let flavour = Flavour::Aggregate;
    let mut d = start(flavour, &[]).await;
    let deadlocks =
        concurrent_group_writes(&mut d, 4, |round, r| round * 20 + (r % 20) as i32 + 1).await;
    assert_oracle(&mut d, flavour).await;
    assert!(
        deadlocks.is_empty(),
        "deadlocks detected while draining:\n{}",
        deadlocks.join("\n--\n")
    );
}

/// #550: key 1 is quarantined. It moves a → z (C1); a Re-derive of z counts
/// it there; it moves z → b (C2); C1, C2 and a `Recompute` of key 1 fold into
/// one record, which the drain parks. Released, the replay must leave z
/// without key 1.
async fn issue_550(flavour: Flavour) {
    let mut d = start(flavour, A_Z_B).await;
    d.ctl
        .execute(
            "insert into poison (src_table, key, last_error) values ($1, '1', 'test')",
            &[&SRC],
        )
        .await
        .expect("poison key 1");
    d.stage_recomputes(SRC, &["5"]).await;
    let r_batch = d.seal().await;
    write(&d, "update public.src set g = 2 where id = 1").await;
    d.drain(r_batch, "a").await;
    write(&d, "update public.src set g = 3 where id = 1").await;
    d.stage_recomputes(SRC, &["1"]).await;
    let c_batch = d.seal().await;
    d.drain(c_batch, "b").await;
    trellis::staging::release_key(d.pool(), SRC, "1")
        .await
        .expect("release key 1");
    assert_oracle(&mut d, flavour).await;
}

/// Before the ledger (#623 D3): z kept key 1, `(2,60,2)` against the
/// oracle's `(2,50,1)`, as the unpoisoned [`issue_494_aggregate`] did. Now
/// the release stages a Re-derive of key 1 and discards the parked rows.
#[tokio::test]
async fn issue_550_aggregate() {
    issue_550(Flavour::Aggregate).await;
}

#[tokio::test]
async fn issue_550_one_to_one() {
    issue_550(Flavour::OneToOne).await;
}

// ------------------------------------------------------------ C's Q7 / #680

/// C's Q7 (#680): an application `AFTER ROW` trigger rewrites the row its
/// own statement wrote. The nested statement's capture runs first, so the
/// ring holds key 1's images out of write order.
async fn nested_same_key(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 3, 10), (2, 3, 5)]).await;
    d.ctl
        .batch_execute(
            "create function public.src_bounce() returns trigger language plpgsql as $$ \
             begin \
               if new.g = 1 then update public.src set g = 2 where id = new.id; end if; \
               return null; \
             end $$; \
             create trigger src_bounce after update on public.src \
               for each row execute function public.src_bounce()",
        )
        .await
        .expect("create the application trigger");
    write(&d, "update public.src set g = 1, v = 30 where id = 1").await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_oracle(&mut d, flavour).await;
}

/// Today: `[(3,15,2)]` against the oracle's `[(2,30,1), (3,5,1)]`: key 1
/// never leaves group 3 and never reaches group 2. #680 tracks the
/// limitation until D8's NEW-only capture.
#[tokio::test]
#[ignore = "#623 D8"]
async fn nested_same_key_aggregate() {
    nested_same_key(Flavour::Aggregate).await;
}

/// Passes today only because the 1-1 apply re-reads the live row under its
/// stripe lock (#344): the last captured image is key 1 in group 1, not 2.
#[tokio::test]
async fn nested_same_key_one_to_one() {
    nested_same_key(Flavour::OneToOne).await;
}

// ------------------------------------------------ chunked_read_exact_point

/// The user's Q1 on the D split: every live read records the exact point it
/// read at, per chunk. The key space is re-derived in two chunks, keys 1–3
/// then keys 4–6, at two different snapshots, with group 1 spanning both.
///
/// - W_in is in flight across both chunk reads: it updates key 2 (chunk 1)
///   and key 5 (chunk 2) before chunk 1 reads and commits after chunk 2
///   reads. Neither read sees it, so both of its changes must apply.
/// - W_between commits between the two reads: it updates key 1 (chunk 1,
///   already read: must apply) and key 4 (chunk 2, not yet read: chunk 2's
///   read sees it, so it must not count twice), and moves key 6 (chunk 2)
///   from group 2 to group 1.
///
/// Chunk 2's worker is frozen after its read while W_in commits and both
/// writers' CDC (one batch, sealed after W_in commits) drains on a second
/// worker, which must queue behind the frozen chunk (I1) and then apply
/// exactly the changes chunk 2's read didn't see.
///
/// Both chunks are sealed before W_in writes, because a writer may straddle
/// only one seal (the seal gate).
///
/// On `main` it passes because the aggregate re-derives whole groups live and
/// the horizon re-derives every later change (`ignore_recompute_horizon`
/// fails it), not because of a per-chunk basis. Under the ledger (D3, D6) it
/// fails if a Re-derive marks as seen a change it didn't see:
///
/// - `applied_lsn` advanced to the read's position drops both of W_in's
///   changes, because their trigger `lsn`s precede both reads;
/// - a basis taken in a statement after the `AfterRederiveRead` hook sees
///   W_in committed and drops its key-5 change.
///
/// It can't see two other mistakes:
///
/// - re-applying a change the read already saw (keys 4 and 6): a ledger
///   Apply sets the entry to its image, so the same image twice moves
///   nothing;
/// - a basis taken in a separate statement between the read and the hook,
///   since nothing commits there.
///
/// So the ledger path keeps the hook directly after its one
/// read-and-snapshot statement, and the aggregate flavour asserts on the
/// stored entries ([`assert_chunk_bases`]): W_between visible in chunk 2's
/// basis and not in chunk 1's, W_in visible in neither, and key 3 (only ever
/// re-derived) with its `applied_lsn` unchanged by either Re-derive. The 1-1
/// flavour's entries are D6's.
async fn chunked_read_exact_point(flavour: Flavour) {
    let mut d = start(
        flavour,
        &[
            (1, 1, 1),
            (2, 1, 2),
            (3, 2, 3),
            (4, 1, 4),
            (5, 1, 5),
            (6, 2, 6),
        ],
    )
    .await;
    d.stage_recomputes(SRC, &["1", "2", "3"]).await;
    let chunk_1 = d.seal().await;
    d.stage_recomputes(SRC, &["4", "5", "6"]).await;
    let chunk_2 = d.seal().await;

    let applied_before = applied_lsn(&d, flavour, "3").await;
    let w_in = d.user().await;
    w_in.batch_execute(
        "begin; \
         update public.src set v = v + 100 where id = 2; \
         update public.src set v = v + 1000 where id = 5",
    )
    .await
    .expect("open W_in");
    let w_in_xid = xact_id(&w_in).await;
    d.drain(chunk_1, "chunk-1").await;
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
        .drain_frozen(
            chunk_2,
            "chunk-2",
            &[(PausePoint::AfterRederiveRead, flavour.target())],
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
    if flavour.on_ledger() {
        assert_chunk_bases(&d, &w_in_xid, &w_between_xid).await;
        assert_eq!(
            applied_lsn(&d, flavour, "3").await,
            applied_before,
            "a Re-derive leaves the entry's applied_lsn alone (the D split's Q1)"
        );
    }
    assert_oracle(&mut d, flavour).await;
}

/// The open transaction's id on `client`, as text.
async fn xact_id(client: &tokio_postgres::Client) -> String {
    client
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("read the transaction id")
        .get(0)
}

/// Key `key`'s ledger `applied_lsn`, as text (`None` when unset), for a
/// target on the ledger.
async fn applied_lsn(d: &Driver, flavour: Flavour, key: &str) -> Option<String> {
    if !flavour.on_ledger() {
        return None;
    }
    d.ctl
        .query_one(
            "select __applied_lsn::text from public.agg__ledger where __from_key = $1",
            &[&key],
        )
        .await
        .expect("read an entry's applied_lsn")
        .get(0)
}

/// The entries [`chunked_read_exact_point`] left: chunk 1's keys (1–3) all
/// carry one basis and chunk 2's (4–6) another, W_between is visible in
/// chunk 2's and not chunk 1's, and W_in in neither. The CDC's Applies left
/// every basis as its chunk's Re-derive wrote it.
async fn assert_chunk_bases(d: &Driver, w_in: &str, w_between: &str) {
    let rows = d
        .ctl
        .query(
            "select __from_key, __basis::text, \
                    pg_visible_in_snapshot($1::text::xid8, __basis), \
                    pg_visible_in_snapshot($2::text::xid8, __basis) \
             from public.agg__ledger where __from_key = any($3) order by __from_key",
            &[&w_in, &w_between, &vec!["1", "2", "3", "4", "5", "6"]],
        )
        .await
        .expect("read the chunks' entries");
    let entries: Vec<(String, String, bool, bool)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    assert_eq!(entries.len(), 6, "{entries:?}");
    let (chunk_1, chunk_2) = entries.split_at(3);
    for chunk in [chunk_1, chunk_2] {
        assert!(
            chunk.iter().all(|e| e.1 == chunk[0].1),
            "one basis per chunk: {entries:?}"
        );
    }
    assert_ne!(chunk_1[0].1, chunk_2[0].1, "{entries:?}");
    for (key, _, w_in_seen, w_between_seen) in &entries {
        assert!(!w_in_seen, "W_in is in flight across both reads, key {key}");
        let chunk_2_key = key.as_str() >= "4";
        assert_eq!(
            *w_between_seen, chunk_2_key,
            "W_between commits between the reads, key {key}: {entries:?}"
        );
    }
}

#[tokio::test]
async fn chunked_read_exact_point_aggregate() {
    chunked_read_exact_point(Flavour::Aggregate).await;
}

#[tokio::test]
async fn chunked_read_exact_point_one_to_one() {
    chunked_read_exact_point(Flavour::OneToOne).await;
}

// ------------------------------------------- chained prior images (#623 D3)

/// A reader chained off a ledger target (a `MAX`, itself on the ledger since
/// #623 D4) finds the groups a write moved away from through each written
/// group's prior image, which the ledger path rebuilds from the upsert's
/// result minus the page's increments. One page moves both of group 1's rows
/// out and two new rows in (emptied and refilled), empties group 3 (deleted),
/// and grows group 2, so the chained groups by `n` see each kind of prior.
#[tokio::test]
async fn a_chained_reader_follows_groups_emptied_and_refilled_in_one_page() {
    // An integer `g`: a numeric group key can't key a chained reader.
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, v numeric); \
         insert into public.src values (1, 1, 10), (3, 1, 7), (5, 2, 1), (6, 3, 4)",
        &[("id", int4), ("g", int4), ("v", ValueType::Numeric)],
        &[Flavour::Aggregate.definition()],
        &[SRC],
    )
    .await;
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM hi FROM public.agg GROUP BY n SELECT MAX(total) AS top",
        &std::collections::HashMap::from([
            ("g".to_string(), int4),
            ("total".to_string(), ValueType::Numeric),
            (
                "n".to_string(),
                ValueType::Integer(trellis::integer::IntWidth::Int8),
            ),
        ]),
        "public",
    )
    .await
    .expect("install the chained reader");
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    write(
        &d,
        "update public.src set g = 2 where id in (1, 3, 6); \
         insert into public.src values (2, 1, 5), (4, 1, 6)",
    )
    .await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_oracle(&mut d, Flavour::Aggregate).await;
    assert_eq!(
        d.rows("select n, top from public.hi order by n").await,
        d.rows(
            "select n, max(total) from \
             (select g, sum(v) as total, count(*) as n from public.src group by g) s \
             group by n order by n"
        )
        .await,
        "the chained reader (left) differs from the oracle (right)"
    );
}

// -------------------------------------------- I2's visibility term (#623 D3)

/// Two updates of key 1 commit: C1 in one batch, then C2 in a second batch
/// with a `Recompute` of key 1, which folds C2 into a Re-derive. That drains
/// first and reads C2's row; C1's batch drains after it. C1 is visible in
/// the entry's basis, so it is skipped. Without the visibility term (the
/// `lsn_only_skip` plant) C1 is still newer than the entry's `applied_lsn`,
/// which a Re-derive leaves alone, so it applies and puts key 1 back to 15.
/// C2 never applies on its own (its Re-derive counted it), so nothing
/// repairs it.
#[tokio::test]
async fn a_change_the_rederive_read_is_not_applied_again_aggregate() {
    let flavour = Flavour::Aggregate;
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    write(&d, "update public.src set v = 15 where id = 1").await;
    let c1 = d.seal().await;
    write(&d, "update public.src set v = 17 where id = 1").await;
    d.stage_recomputes(SRC, &["1"]).await;
    let c2_and_r = d.seal().await;
    d.drain(c2_and_r, "a").await;
    d.drain(c1, "b").await;
    assert_oracle(&mut d, flavour).await;
}

// --------------------------------------- group keys and increments (#623 D3)

/// A `numeric` group key whose rows spell one value at two scales (`1.5`,
/// `1.50`) is one group, whose row keeps the spelling that created it. The
/// page that empties it through the other spelling must still delete it:
/// matching the upserted row to its increments by text left it behind as
/// `(1.5, NULL, 0)`.
#[tokio::test]
async fn a_group_emptied_through_another_spelling_of_its_key_is_deleted() {
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g numeric, v numeric); \
         insert into public.src values (1, 1.5, 10)",
        &[
            ("id", int4),
            ("g", ValueType::Numeric),
            ("v", ValueType::Numeric),
        ],
        &[Flavour::Aggregate.definition()],
        &[SRC],
    )
    .await;
    write(&d, "insert into public.src values (2, 1.50, 20)").await;
    let b = d.seal().await;
    d.drain(b, "a").await;
    write(&d, "delete from public.src where id = 1").await;
    let b = d.seal().await;
    d.drain(b, "b").await;
    write(&d, "delete from public.src where id = 2").await;
    let b = d.seal().await;
    d.drain(b, "c").await;
    assert_oracle(&mut d, Flavour::Aggregate).await;
}

/// An `integer` contribution of -2147483648 leaves its group. Negating it
/// as an `integer` (`sum(-1 * v)`) failed the page with `integer out of
/// range`.
#[tokio::test]
async fn the_minimum_integer_contribution_leaves_its_group() {
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, v integer); \
         insert into public.src values (1, 1, -2147483648), (2, 1, 5)",
        &[("id", int4), ("g", int4), ("v", int4)],
        &[Flavour::Aggregate.definition()],
        &[SRC],
    )
    .await;
    write(&d, "update public.src set g = 2 where id = 1").await;
    let b = d.seal().await;
    d.drain(b, "a").await;
    assert_oracle(&mut d, Flavour::Aggregate).await;
}

// ------------------------------------------------- the AVG flavour (#623 D3b)

// The scenarios above with `AVG` beside the `SUM` whose hidden count it
// shares, and a `COUNT(v)`: on the ledger since #623 D3b. Before it the
// target was on the old path, where exp2_5, exp2_5b and issue_550 fail as
// their aggregate flavours' doc comments describe.

#[tokio::test]
async fn pause_points_fire_in_page_order_aggregate_avg() {
    pause_points_fire_in_page_order(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn exp2_2_aggregate_avg() {
    exp2_2(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn exp2_2b_aggregate_avg() {
    exp2_2b(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn exp2_5_aggregate_avg() {
    exp2_5(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn exp2_5b_aggregate_avg() {
    exp2_5b(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn exp2_9_aggregate_avg() {
    exp2_9(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn exp2_9b_aggregate_avg() {
    exp2_9b(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn exp2_9c_aggregate_avg() {
    exp2_9c(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn exp2_9d_aggregate_avg() {
    exp2_9d(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn exp2_10_aggregate_avg() {
    exp2_10(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn issue_392_aggregate_avg() {
    issue_392(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn issue_550_aggregate_avg() {
    issue_550(Flavour::AggregateAvg).await;
}

#[tokio::test]
async fn chunked_read_exact_point_aggregate_avg() {
    chunked_read_exact_point(Flavour::AggregateAvg).await;
}

/// `AVG` on the ledger equals Postgres's `avg()` over the source, as text,
/// for `integer`, `bigint` and `numeric` arguments: the same `numeric`
/// result type and scale. The pages move rows between groups, null an
/// argument, delete rows, take a group's non-null count to 0 while it keeps
/// members (`AVG` goes NULL, `COUNT(*)` stays), re-derive keys, and carry the
/// `integer` minimum, `bigint` values whose sum overflows `bigint`, and
/// `numeric`s of three scales, including removing the widest.
#[tokio::test]
async fn avg_equals_postgres_avg_over_integer_bigint_and_numeric() {
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    let int8 = ValueType::Integer(trellis::integer::IntWidth::Int8);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, a integer, b bigint, \
                                  c numeric, name text); \
         insert into public.src values \
             (1, 1, -2147483648, 9223372036854775807, 1.5, 'x'), \
             (2, 1, 7, 9223372036854775806, 2.125, null), \
             (3, 2, null, null, 0.1, 'y'), \
             (4, 2, 3, 4, null, 'z')",
        &[
            ("id", int4),
            ("g", int4),
            ("a", int4),
            ("b", int8),
            ("c", ValueType::Numeric),
            ("name", ValueType::Text),
        ],
        &["TRANSFORM agg FROM public.src GROUP BY g \
           SELECT AVG(a) AS avg_a, AVG(b) AS avg_b, AVG(c) AS avg_c, COUNT(name) AS named, \
                  COUNT(*) AS n"],
        &[SRC],
    )
    .await;
    let actual = "select g, avg_a, avg_b, avg_c, named, n from public.agg order by g";
    let expected = "select g, avg(a), avg(b), avg(c), count(name), count(*) \
                    from public.src group by g order by g";
    let steps = [
        "insert into public.src values (5, 1, 2147483647, -1, 10.12345, 'w'); \
         update public.src set g = 2 where id = 2",
        "delete from public.src where id = 2; \
         update public.src set a = null, b = null, c = null, name = null where id = 4",
        "update public.src set a = null, b = null, c = null where id = 3; \
         insert into public.src values (6, 3, 1, 1, 1, null)",
        "update public.src set g = 3, c = 2.5 where id = 5",
    ];
    for (i, step) in steps.iter().enumerate() {
        write(&d, step).await;
        if i == 2 {
            d.stage_recomputes(SRC, &["1", "4", "5"]).await;
        }
        let b = d.seal().await;
        d.drain(b, &format!("w{i}")).await;
        d.settle().await;
        assert_eq!(
            d.rows(actual).await,
            d.rows(expected).await,
            "after step {i}: the target (left) differs from the oracle (right)"
        );
    }
    assert!(
        d.rows("select 1 from public.agg where g = 2 and avg_a is null and n = 2")
            .await
            .len()
            == 1,
        "group 2 keeps its members with no non-null argument"
    );
    assert_eq!(
        d.rows(
            "select __from_key from public.agg__ledger where __applied_lsn is not null order by 1"
        )
        .await,
        ["(3)", "(4)", "(5)", "(6)"],
        "the target is on the ledger: every surviving key a change reached has an applied \
         entry (key 1 was only re-derived)"
    );
    assert!(
        d.rows("select 1 from public.agg__ledger where __from_key = '2'")
            .await
            .is_empty(),
        "key 2's own delete tombstoned it, and every `settle` call runs GC (#623 D7): by \
         now nothing lags behind it, so its entry is gone rather than merely applied"
    );
}

// --------------------------------------------- the MIN/MAX flavour (#623 D4)

// The scenarios above over `MIN`/`MAX` beside a `COUNT(*)`: on the ledger
// since #623 D4, which recomputes each written group's `MIN`/`MAX` from its
// live entries. Before it the target was on the old path, where exp2_5
// failed (see `exp2_5_aggregate_min_max`).

#[tokio::test]
async fn pause_points_fire_in_page_order_aggregate_min_max() {
    pause_points_fire_in_page_order(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn exp2_2_aggregate_min_max() {
    exp2_2(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn exp2_2b_aggregate_min_max() {
    exp2_2b(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn exp2_5b_aggregate_min_max() {
    exp2_5b(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn exp2_9_aggregate_min_max() {
    exp2_9(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn exp2_9b_aggregate_min_max() {
    exp2_9b(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn exp2_9c_aggregate_min_max() {
    exp2_9c(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn exp2_9d_aggregate_min_max() {
    exp2_9d(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn exp2_10_aggregate_min_max() {
    exp2_10(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn issue_392_aggregate_min_max() {
    issue_392(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn issue_550_aggregate_min_max() {
    issue_550(Flavour::AggregateMinMax).await;
}

#[tokio::test]
async fn chunked_read_exact_point_aggregate_min_max() {
    chunked_read_exact_point(Flavour::AggregateMinMax).await;
}

/// Drives `steps` through `d`, one sealed page each (with `recompute` keys
/// staged into the page at `recompute.0`), comparing `actual` to `expected`
/// as text after each, then asserts every key a change reached has an
/// applied ledger entry: the target is on the ledger.
async fn assert_steps_match(
    d: &mut Driver,
    actual: &str,
    expected: &str,
    steps: &[&str],
    recompute: (usize, &[&str]),
) {
    assert_eq!(
        d.rows(actual).await,
        d.rows(expected).await,
        "after the build: the target (left) differs from the oracle (right)"
    );
    for (i, step) in steps.iter().enumerate() {
        write(d, step).await;
        if i == recompute.0 {
            d.stage_recomputes(SRC, recompute.1).await;
        }
        let b = d.seal().await;
        d.drain(b, &format!("w{i}")).await;
        d.settle().await;
        assert_eq!(
            d.rows(actual).await,
            d.rows(expected).await,
            "after step {i}: the target (left) differs from the oracle (right)"
        );
    }
    assert!(
        !d.rows("select 1 from public.agg__ledger where __applied_lsn is not null")
            .await
            .is_empty(),
        "the target is on the ledger"
    );
}

/// `BOOL_AND`/`BOOL_OR` recomputed from the ledger equal Postgres's over the
/// source through NULL arguments: a group whose every argument is NULL
/// (both NULL, `COUNT(*)` counting it), one that becomes all-NULL, and one
/// that leaves it.
#[tokio::test]
async fn bool_and_and_bool_or_equal_postgres_over_nulls_and_all_null_groups() {
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, b boolean); \
         insert into public.src values (1, 1, true), (2, 1, null), (3, 2, null), (4, 2, null), \
                                       (5, 3, false), (6, 3, true)",
        &[("id", int4), ("g", int4), ("b", ValueType::Boolean)],
        &["TRANSFORM agg FROM public.src GROUP BY g \
           SELECT BOOL_AND(b) AS all_b, BOOL_OR(b) AS any_b, COUNT(*) AS n"],
        &[SRC],
    )
    .await;
    assert_steps_match(
        &mut d,
        "select g, all_b, any_b, n from public.agg order by g",
        "select g, bool_and(b), bool_or(b), count(*) from public.src group by g order by g",
        &[
            "update public.src set b = false where id = 1; \
             insert into public.src values (7, 2, null)",
            "update public.src set b = true where id = 3; delete from public.src where id = 5",
            "update public.src set b = null where id in (1, 3, 6)",
            "update public.src set g = 1, b = false where id = 6; \
             update public.src set b = true where id = 4",
        ],
        (2, &["2", "4"]),
    )
    .await;
}

/// Float `MIN`/`MAX`/`SUM` recomputed from the ledger equal Postgres's over
/// the source through NaN (which sorts above every number) and ±Infinity,
/// whose sum is NaN, for `double precision` and `real`. The values are
/// exact, so summation order can't differ.
#[tokio::test]
async fn float_min_max_and_sum_equal_postgres_through_nan_and_infinities() {
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, f double precision, \
                                  r real); \
         insert into public.src values (1, 1, 1.5, 1.5), (2, 1, 'NaN', 'NaN'), \
             (3, 2, 'Infinity', 'Infinity'), (4, 2, '-Infinity', '-Infinity'), \
             (5, 3, 2, 2), (6, 3, null, null)",
        &[
            ("id", int4),
            ("g", int4),
            ("f", ValueType::Float(trellis::FloatWidth::Float8)),
            ("r", ValueType::Float(trellis::FloatWidth::Float4)),
        ],
        &["TRANSFORM agg FROM public.src GROUP BY g \
           SELECT MIN(f) AS lo, MAX(f) AS hi, SUM(f) AS total, MIN(r) AS rlo, MAX(r) AS rhi, \
                  SUM(r) AS rtotal, COUNT(*) AS n"],
        &[SRC],
    )
    .await;
    assert_steps_match(
        &mut d,
        "select g, lo::text, hi::text, total::text, rlo::text, rhi::text, rtotal::text, n \
         from public.agg order by g",
        "select g, min(f)::text, max(f)::text, sum(f)::text, min(r)::text, max(r)::text, \
                sum(r)::text, count(*) from public.src group by g order by g",
        &[
            "delete from public.src where id = 2",
            "update public.src set f = '-Infinity', r = '-Infinity' where id = 3; \
             insert into public.src values (7, 1, 'NaN', 'NaN')",
            "update public.src set g = 3 where id = 4; \
             update public.src set f = null, r = null where id = 5",
            "update public.src set f = 'Infinity', r = 'Infinity' where id = 1; \
             delete from public.src where id = 7",
        ],
        (1, &["1", "6"]),
    )
    .await;
}

/// `numeric` `MIN`/`MAX` recomputed from the ledger keep the scale of the
/// value Postgres picks over the source (`2.125`, `0.10`, `1000.000`), as the
/// extremes move between values of different scales.
#[tokio::test]
async fn numeric_min_max_keep_each_extremes_own_scale() {
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, c numeric); \
         insert into public.src values (1, 1, 1.5), (2, 1, 2.125), (3, 1, 10), \
                                       (4, 2, -0.001), (5, 2, 100.10)",
        &[("id", int4), ("g", int4), ("c", ValueType::Numeric)],
        &["TRANSFORM agg FROM public.src GROUP BY g \
           SELECT MIN(c) AS lo, MAX(c) AS hi, COUNT(*) AS n"],
        &[SRC],
    )
    .await;
    assert_steps_match(
        &mut d,
        "select g, lo::text, hi::text, n from public.agg order by g",
        "select g, min(c)::text, max(c)::text, count(*) from public.src group by g order by g",
        &[
            "delete from public.src where id = 3",
            "update public.src set c = 0.10 where id = 1",
            "delete from public.src where id = 5; insert into public.src values (6, 2, 1000.000)",
            "update public.src set g = 1 where id = 6",
        ],
        (1, &["2", "4"]),
    )
    .await;
}

/// Composed fields and expression arguments on the ledger: an expression
/// over two aggregates (`SUM(a) + COUNT(b)`, `MIN(b) > MAX(a)`), aggregates
/// of expressions (`MAX(a + 1)`, `SUM(a + b)`), and `COALESCE` over one,
/// each equal to the same SQL over the source. The expression arguments are
/// evaluated per change into their ledger columns. (The grammar has `+` and
/// `>` only, so there is no `SUM(a) / COUNT(b)`.)
#[tokio::test]
async fn composed_fields_and_expression_arguments_equal_postgres() {
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, a integer, b integer); \
         insert into public.src values (1, 1, 1, 10), (2, 1, 5, null), (3, 2, null, null), \
                                       (4, 2, 2, 3)",
        &[("id", int4), ("g", int4), ("a", int4), ("b", int4)],
        &["TRANSFORM agg FROM public.src GROUP BY g \
           SELECT SUM(a) + COUNT(b) AS mixed, MAX(a + 1) AS top, SUM(a + b) AS ab, \
                  MIN(b) > MAX(a) AS apart, COALESCE(MIN(b), 0) AS low_b, COUNT(*) AS n"],
        &[SRC],
    )
    .await;
    assert_steps_match(
        &mut d,
        "select g, mixed::text, top::text, ab::text, apart, low_b::text, n \
         from public.agg order by g",
        "select g, (sum(a) + count(b))::text, max(a + 1::numeric)::text, sum(a + b)::text, \
                min(b) > max(a), coalesce(min(b), 0::numeric)::text, count(*) \
         from public.src group by g order by g",
        &[
            "update public.src set a = 9 where id = 1; insert into public.src values (5, 2, 7, 1)",
            "update public.src set b = null where id in (1, 4); \
             update public.src set g = 1 where id = 5",
            "delete from public.src where id = 2; update public.src set a = null where id = 5",
            "update public.src set a = 4, b = 4 where id = 3",
        ],
        (2, &["1", "3"]),
    )
    .await;
}

/// `MIN`/`MAX` recomputed from the ledger over a two-column `GROUP BY` with
/// `NULL` keys: a group with one `NULL` column, one with both, and fully
/// keyed groups beside them, each recomputed from its own entries only (the
/// `NULL`-matching branch must not leak one group's entries into another's).
#[tokio::test]
async fn min_max_over_null_group_keys_recompute_each_group_from_its_own_entries() {
    let int4 = ValueType::Integer(trellis::integer::IntWidth::Int4);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, h text, v integer); \
         insert into public.src values (1, 1, 'a', 10), (2, 1, null, 20), (3, null, 'a', 30), \
                                       (4, null, null, 40), (5, 1, 'a', 5), (6, null, null, 1)",
        &[
            ("id", int4),
            ("g", int4),
            ("h", ValueType::Text),
            ("v", int4),
        ],
        &["TRANSFORM agg FROM public.src GROUP BY g, h \
           SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n"],
        &[SRC],
    )
    .await;
    assert_steps_match(
        &mut d,
        "select g, h, lo, hi, n from public.agg order by g, h",
        "select g, h, min(v), max(v), count(*) from public.src group by g, h order by g, h",
        &[
            "update public.src set v = 50 where id in (2, 3); delete from public.src where id = 4",
            "update public.src set h = null where id = 5; \
             insert into public.src values (7, null, null, 99)",
            "update public.src set g = null, h = null where id = 1; \
             update public.src set v = -3 where id = 6",
        ],
        (1, &["3", "6"]),
    )
    .await;
}

// ------------------------------------------------------------ issue #690

/// Issue #690: a claim whose statement stalls between its snapshot and its
/// insert, while a peer claims, drains and completes the same segment, must
/// not claim the buckets the peer drained, and nobody deadlocks.
///
/// Worker c holds one of the batch's 8 buckets first, so the segment is
/// already `draining` and b's `sealed -> draining` flip locks nothing. A
/// trigger on `seg_claims` stalls b's claim at its first insert, on an
/// advisory lock the test holds, and logs every claim row that lands with
/// whether its bucket was already drained. a then claims, and the test lets b
/// go once a has either finished its drain or queued behind b.
///
/// Before the fix b computed its free buckets from its statement snapshot,
/// taken before a claimed: a claimed, drained and released the other 7
/// buckets, then b claimed all 7 again and re-drained them.
#[tokio::test]
async fn a_stalled_claim_does_not_reclaim_buckets_a_peer_drained_meanwhile() {
    let flavour = Flavour::Aggregate;
    let mut d = start(flavour, &[]).await;
    let deadlocks_before = d.deadlocks_logged().len();
    write(
        &d,
        "insert into public.src select i, i % 20 + 1, i from generate_series(1, 320) as i",
    )
    .await;
    let batch = d.seal().await;
    assert_eq!(
        d.rows(&format!(
            "select bucket_count from segments where seg_seq = {batch}"
        ))
        .await,
        vec!["(8)"],
        "the batch is split"
    );

    let mut c = d.user().await;
    let txn = c.transaction().await.expect("begin c's claim");
    let c_won = claim::claim(&txn, batch, "c", 8).await.expect("c claims");
    txn.commit().await.expect("commit c's claim");
    assert_eq!(c_won.len(), 1, "c holds one bucket: {c_won:?}");

    d.ctl
        .batch_execute(&format!(
            "create table public.claim_log \
                 (claimed_by text, bucket smallint, already_drained boolean); \
             create function public.stall_b() returns trigger language plpgsql as $$ \
             begin \
                 if new.claimed_by = 'b' then \
                     perform set_config('lock_timeout', '0', true); \
                     perform pg_advisory_xact_lock_shared(690); \
                 end if; \
                 return new; \
             end $$; \
             create function public.log_claim() returns trigger language plpgsql as $$ \
             begin \
                 insert into public.claim_log \
                 select new.claimed_by, new.bucket, \
                        (s.drained_mask & (1::bigint << new.bucket)) <> 0 \
                 from {DEFAULT_SCHEMA}.segments s where s.seg_seq = new.seg_seq; \
                 return new; \
             end $$; \
             create trigger stall_b before insert on {DEFAULT_SCHEMA}.seg_claims \
                 for each row execute function public.stall_b(); \
             create trigger log_claim after insert on {DEFAULT_SCHEMA}.seg_claims \
                 for each row execute function public.log_claim();"
        ))
        .await
        .expect("install the stall and the claim log");

    let gate = d.user().await;
    gate.execute("select pg_advisory_lock(690)", &[])
        .await
        .expect("take the stall lock");
    let gate_pid: i32 = gate
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("gate pid")
        .get(0);

    let drain = |worker: &'static str| {
        let pool = d.pool().clone();
        tokio::spawn(async move {
            apply::drain_once(
                &pool,
                batch,
                worker,
                1,
                drain_driver::WAKE,
                &StagedWatermark::saturated(),
            )
            .await
        })
    };
    let b = drain("b");
    d.wait_blocked_behind(gate_pid).await;
    let b_pid: i32 = d
        .ctl
        .query_one(
            "select pid from pg_stat_activity where $1 = any(pg_blocking_pids(pid))",
            &[&gate_pid],
        )
        .await
        .expect("b's backend")
        .get(0);

    // a either runs to the end (b holds nothing it needs) or queues behind b.
    let a = drain("a");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let a_waits_on_b: bool = d
            .ctl
            .query_one(
                "select exists (select 1 from pg_stat_activity \
                 where $1 = any(pg_blocking_pids(pid)))",
                &[&b_pid],
            )
            .await
            .expect("read pg_stat_activity")
            .get(0);
        if a.is_finished() || a_waits_on_b {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "a neither finished nor queued behind b within 60 s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    gate.execute("select pg_advisory_unlock(690)", &[])
        .await
        .expect("release b");
    b.await.expect("b's task").expect("b's drain");
    a.await.expect("a's task").expect("a's drain");

    let re_claimed = d
        .rows("select claimed_by, bucket from public.claim_log where already_drained order by 1, 2")
        .await;
    assert!(
        re_claimed.is_empty(),
        "claims of already-drained buckets: {re_claimed:?}"
    );
    let twice = d
        .rows("select bucket, count(*) from public.claim_log group by bucket having count(*) > 1")
        .await;
    assert!(twice.is_empty(), "buckets claimed twice: {twice:?}");
    let deadlocks = d.deadlocks_logged().split_off(deadlocks_before);
    assert!(
        deadlocks.is_empty(),
        "deadlocks detected:\n{}",
        deadlocks.join("\n--\n")
    );

    d.ctl
        .batch_execute(&format!(
            "drop trigger stall_b on {DEFAULT_SCHEMA}.seg_claims; \
             drop trigger log_claim on {DEFAULT_SCHEMA}.seg_claims"
        ))
        .await
        .expect("drop the triggers");
    d.drain(batch, "c").await;
    assert_oracle(&mut d, flavour).await;
}

// ------------------------------------------------------ tombstone GC (D7)
//
// `trellis::staging::collect_tombstones` deletes the tombstones at or below
// the contiguous drained prefix. Each scenario also fails under the
// `early_tombstone_gc` plant, which collects through the highest drained
// segment instead.

/// The keys of `public.agg`'s ledger tombstones, as `(key)` rows.
async fn tombstones(d: &Driver) -> Vec<String> {
    d.rows("select __from_key from public.agg__ledger where __tombstone order by 1")
        .await
}

/// Exp 2 scenario 9 with a GC between the delete and its older update: the
/// update's batch lags, so the delete's tombstone must outlive the GC, or
/// the update applies to a fresh entry and brings key 1 back.
async fn exp2_9_gc_waits_for_the_older_update(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    write(&d, "update public.src set v = 15 where id = 1").await;
    let b1 = d.seal().await;
    write(&d, "delete from public.src where id = 1").await;
    let b2 = d.seal().await;
    d.drain(b2, "a").await;
    assert_eq!(tombstones(&d).await, ["(1)"]);
    assert_eq!(d.collect_tombstones().await, 0, "the update's batch lags");
    assert_eq!(tombstones(&d).await, ["(1)"]);
    d.drain(b1, "b").await;
    assert_eq!(d.collect_tombstones().await, 1, "nothing lags any more");
    assert!(tombstones(&d).await.is_empty());
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn exp2_9_gc_waits_for_the_older_update_aggregate() {
    exp2_9_gc_waits_for_the_older_update(Flavour::Aggregate).await;
}

#[tokio::test]
async fn exp2_9_gc_waits_for_the_older_update_aggregate_min_max() {
    exp2_9_gc_waits_for_the_older_update(Flavour::AggregateMinMax).await;
}

/// A batch drained out of order holds GC back at the batch below it: the
/// tombstone of the first batch goes, the one above the lagging batch
/// stays until the lagging batch has drained.
async fn an_out_of_order_drain_holds_gc_back(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20), (3, 2, 5)]).await;
    write(&d, "delete from public.src where id = 3").await;
    let b1 = d.seal().await;
    d.drain(b1, "a").await;
    write(&d, "update public.src set v = 15 where id = 1").await;
    let b2 = d.seal().await;
    write(&d, "delete from public.src where id = 1").await;
    let b3 = d.seal().await;
    d.drain(b3, "a").await;
    assert_eq!(tombstones(&d).await, ["(1)", "(3)"]);
    assert_eq!(
        d.collect_tombstones().await,
        1,
        "only the batch below the lagging one is in the drained prefix"
    );
    assert_eq!(tombstones(&d).await, ["(1)"]);
    d.drain(b2, "b").await;
    assert_eq!(d.collect_tombstones().await, 1);
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn an_out_of_order_drain_holds_gc_back_aggregate() {
    an_out_of_order_drain_holds_gc_back(Flavour::Aggregate).await;
}

#[tokio::test]
async fn an_out_of_order_drain_holds_gc_back_aggregate_min_max() {
    an_out_of_order_drain_holds_gc_back(Flavour::AggregateMinMax).await;
}

/// GC under a concurrent re-insert of a deleted key: the re-insert's page is
/// frozen after its placeholder insert, which found key 1's tombstone and
/// so inserted nothing. The GC collects that tombstone before the page's
/// entry lock, which then finds no entry for key 1 and must take the lock
/// again, placeholders first, or the re-insert is lost. The same page holds
/// an update of key 3 older than key 3's delete, drained in the batch above
/// it, so key 3's tombstone must outlive the GC.
async fn gc_under_a_concurrent_reinsert(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20), (3, 2, 5)]).await;
    write(&d, "delete from public.src where id = 1").await;
    let b1 = d.seal().await;
    d.drain(b1, "a").await;
    write(
        &d,
        "insert into public.src values (1, 1, 30); update public.src set v = 6 where id = 3",
    )
    .await;
    let b2 = d.seal().await;
    write(&d, "delete from public.src where id = 3").await;
    let b3 = d.seal().await;
    d.drain(b3, "a").await;
    assert_eq!(tombstones(&d).await, ["(1)", "(3)"]);
    let mut reinsert = d
        .drain_frozen(
            b2,
            "b",
            &[(PausePoint::AfterPlaceholders, flavour.target())],
        )
        .await;
    reinsert.reached(PausePoint::AfterPlaceholders).await;
    assert_eq!(
        d.collect_tombstones().await,
        1,
        "key 1's tombstone goes; key 3's is above the frozen batch"
    );
    assert_eq!(tombstones(&d).await, ["(3)"]);
    d.release(&mut reinsert, PausePoint::AfterPlaceholders)
        .await;
    reinsert.finish().await;
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn gc_under_a_concurrent_reinsert_aggregate() {
    gc_under_a_concurrent_reinsert(Flavour::Aggregate).await;
}

#[tokio::test]
async fn gc_under_a_concurrent_reinsert_aggregate_min_max() {
    gc_under_a_concurrent_reinsert(Flavour::AggregateMinMax).await;
}
