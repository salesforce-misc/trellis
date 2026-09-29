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
//! the user's Q1 on the D split). Each runs in two flavours where the shape
//! applies: a plain `SUM`/`COUNT` aggregate and a 1-1 target.
//!
//! A scenario that fails on today's engine is `#[ignore]`d naming the D part
//! that makes it pass; that part un-ignores it. The wrong value each one
//! shows today is in its doc comment.
//!
//! "Re-derive" below is today's live re-read: an image-less `Recompute` for
//! a key, which re-reads the 1-1 row, or re-derives the key's group from a
//! live `GROUP BY` (the forced path).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::staging::interleave::PausePoint;

const SRC: &str = "public.src";

#[derive(Clone, Copy, Debug)]
enum Flavour {
    Aggregate,
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
            Flavour::AggregateMinMax => {
                "TRANSFORM agg FROM public.src GROUP BY g \
                 SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n"
            }
            Flavour::OneToOne => "TRANSFORM one FROM public.src SELECT g AS g, v AS v",
        }
    }

    fn target(self) -> &'static str {
        match self {
            Flavour::Aggregate | Flavour::AggregateMinMax => "public.agg",
            Flavour::OneToOne => "public.one",
        }
    }

    fn actual(self) -> &'static str {
        match self {
            Flavour::Aggregate => "select g, total, n from public.agg order by g",
            Flavour::AggregateMinMax => "select g, lo, hi, n from public.agg order by g",
            Flavour::OneToOne => "select id, g, v from public.one order by id",
        }
    }

    fn expected(self) -> &'static str {
        match self {
            Flavour::Aggregate => {
                "select g, sum(v), count(*) from public.src group by g order by g"
            }
            Flavour::AggregateMinMax => {
                "select g, min(v), max(v), count(*) from public.src group by g order by g"
            }
            Flavour::OneToOne => "select id, g, v from public.src order by id",
        }
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
             alter table public.src replica identity full; {seed}"
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
        PausePoint::AfterEntryLock,
        PausePoint::AfterRederiveRead,
        PausePoint::AfterGroupUpsert,
        PausePoint::BeforeCommit,
    ];
    if matches!(flavour, Flavour::OneToOne) {
        points.retain(|p| *p != PausePoint::AfterGroupUpsert);
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
/// Ignored because it fails on `main` intermittently, not every run: about
/// 1 run in 8 when 8 copies run in parallel,
/// never in 15 runs alone (exp2_4 also hit it once in a full `verify`). The cycle is one drain's bulk group `update` against
/// another's new-group `insert ... on conflict`, each waiting on the other's
/// transaction. The engine retries, so the oracle still holds; the zero
/// deadlocks D3 promises doesn't.
#[tokio::test]
#[ignore = "#623 D3"]
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

/// Today: z keeps key 1, `(2,60,2)` against the oracle's `(2,50,1)`. The
/// folded record names only a and b, so nothing corrects the Re-derive's
/// count of key 1 in z (#494's shape).
#[tokio::test]
#[ignore = "#623 D3"]
async fn exp2_5_aggregate() {
    exp2_5(Flavour::Aggregate).await;
}

/// Today: `(2,10,50,2)` against the oracle's `(2,50,50,1)`, for the same
/// reason as [`exp2_5_aggregate`].
#[tokio::test]
#[ignore = "#623 D4"]
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

/// Today: z ends `(2,10,1)` against the oracle's `(2,50,1)`. Applied first,
/// C2 takes key 1 out of z before C1 has put it in, which drops z's
/// non-null count for `SUM(v)` to 0, so the total resets to NULL although
/// key 5 is still in z; C1's +10 then lands on the reset total.
#[tokio::test]
#[ignore = "#623 D3"]
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
/// Today: z keeps key 1, `(2,60,2)` against the oracle's `(2,50,1)`.
#[tokio::test]
#[ignore = "#623 D3"]
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
/// Ignored because it fails on `main` intermittently, not every run: about
/// 1 run in 8 when 8 copies run in parallel,
/// never in 15 runs alone (exp2_4 also hit it once in a full `verify`). The cycle is one drain's bulk group `update` against
/// another's new-group `insert ... on conflict`, each waiting on the other's
/// transaction. The engine retries, so the oracle still holds; the zero
/// deadlocks D3 promises doesn't.
#[tokio::test]
#[ignore = "#623 D3"]
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

/// Today: z keeps key 1, `(2,60,2)` against the oracle's `(2,50,1)`. The
/// unpoisoned [`issue_494_aggregate`] fails the same way, so on today's
/// engine this doesn't separate #550's parking from #494's fold.
#[tokio::test]
#[ignore = "#623 D3"]
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
///   read sees it, so it must not apply again), and moves key 6 (chunk 2)
///   from group 2 to group 1.
///
/// Chunk 2's worker is frozen after its read while W_in commits and both
/// writers' CDC (one batch, sealed after W_in commits) drains on a second
/// worker, which must queue behind the frozen chunk (I1) and then apply
/// exactly the changes chunk 2's read didn't see.
///
/// Both chunks are sealed before W_in writes, because a writer may straddle
/// only one seal (the seal gate).
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

    let w_in = d.user().await;
    w_in.batch_execute(
        "begin; \
         update public.src set v = v + 100 where id = 2; \
         update public.src set v = v + 1000 where id = 5",
    )
    .await
    .expect("open W_in");
    d.drain(chunk_1, "chunk-1").await;
    write(
        &d,
        "update public.src set v = v + 10 where id = 1; \
         update public.src set v = v + 20 where id = 4; \
         update public.src set g = 1 where id = 6",
    )
    .await;
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
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn chunked_read_exact_point_aggregate() {
    chunked_read_exact_point(Flavour::Aggregate).await;
}

#[tokio::test]
async fn chunked_read_exact_point_one_to_one() {
    chunked_read_exact_point(Flavour::OneToOne).await;
}
