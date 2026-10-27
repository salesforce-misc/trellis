//! Deterministic interleavings of the drain on a 1-1 target's slim side
//! ledger (ADR-0002 invariants I1 and I2).
//!
//! Each test drives one interleaving by hand through
//! `support/drain_driver.rs`: source writes go through trigger capture, the
//! test seals batches itself and drains each one on a worker it names,
//! freezing a worker at a pause point (`trellis::staging::interleave`) where
//! the scenario needs one. No test sleeps or polls for convergence.
//! Every test ends by checking the target against a from-scratch oracle over
//! the source.
//!
//! The scenarios are the design experiment's interleavings, the shapes of
//! the earlier bugs the ledger supersedes, and `chunked_read_exact_point`
//! (the per-chunk exact read point).
//!
//! "Re-derive" below is an image-less `Recompute` for a key. A 1-1 target is
//! on its ledger (`trellis::staging::one_to_one_ledger`), where a Re-derive
//! reads the row and snapshot in one statement and stamps the key's entry.
//! (An aggregate's ordering state is the group basis's, pinned by
//! `group_basis_interleavings.rs`.)

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use tokio_postgres::types::PgLsn;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::staging::interleave::PausePoint;
use trellis::staging::{CdcOp, StagedChange};

const SRC: &str = "public.src";

#[derive(Clone, Copy, Debug)]
enum Flavour {
    OneToOne,
}

impl Flavour {
    fn definition(self) -> &'static str {
        "TRANSFORM one FROM public.src SELECT g AS g, v AS v"
    }

    fn target(self) -> &'static str {
        "public.one"
    }

    fn actual(self) -> &'static str {
        "select id, g, v from public.one order by id"
    }

    fn expected(self) -> &'static str {
        "select id, g, v from public.src order by id"
    }

    /// The flavour's target's ledger.
    fn ledger(self) -> &'static str {
        "public.one__ledger"
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
async fn pause_points_fire_in_page_order_one_to_one() {
    pause_points_fire_in_page_order(Flavour::OneToOne).await;
}

// ---------------------------------------------------------------- exp 2, 2b

/// Exp 2 scenario 2: a Re-derive of key 1 is frozen after its read,
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
async fn exp2_2_one_to_one() {
    exp2_2(Flavour::OneToOne).await;
}

#[tokio::test]
async fn exp2_2b_one_to_one() {
    exp2_2b(Flavour::OneToOne).await;
}

// ------------------------------------------------------------------ exp 2, 3

// ------------------------------------------------------------------ exp 2, 4

// ------------------------------------------------------------------ exp 2, 5

/// Seeds key 1 in group a (1), key 5 in z (2) and key 6 in b (3).
const A_Z_B: &[(i32, i32, i32)] = &[(1, 1, 10), (5, 2, 50), (6, 3, 60)];

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
async fn exp2_9_one_to_one() {
    exp2_9(Flavour::OneToOne).await;
}

#[tokio::test]
async fn exp2_9b_one_to_one() {
    exp2_9b(Flavour::OneToOne).await;
}

#[tokio::test]
async fn exp2_9c_one_to_one() {
    exp2_9c(Flavour::OneToOne).await;
}

#[tokio::test]
async fn exp2_9d_one_to_one() {
    exp2_9d(Flavour::OneToOne).await;
}

/// A source `TRUNCATE` empties a target's ledger, so a change from before
/// it that still reaches a page afterward meets no entry, no basis and no
/// `applied_lsn`: only the truncate floor (the D split's Q6) tells it is
/// older than the truncate. Here one is staged by hand at an `lsn` below the
/// truncate's; applied, it would bring back a row the source no longer has.
/// On either ledger the key's Apply would write its entry in the lock's
/// insert, so that insert must check the floor itself.
///
/// The refused change leaves no entry behind: the lock's insert gave
/// its key a placeholder, which nothing then writes, and which the
/// tombstone GC would never collect.
async fn a_change_from_before_a_truncate_does_not_come_back(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    write(&d, "truncate public.src").await;
    d.settle().await;
    assert_eq!(d.rows(flavour.actual()).await, Vec::<String>::new());

    stage_pre_truncate_insert(&d, "1", r#"{"id":"1","g":"1","v":"10"}"#).await;
    assert_oracle(&mut d, flavour).await;
    assert_eq!(
        d.rows(&format!("select __from_key from {}", flavour.ledger()))
            .await,
        Vec::<String>::new(),
        "the refused change left an entry behind"
    );
}

/// Stages an insert of `key` with `image` at `lsn` 1, below any truncate's
/// floor: a change from before the truncate that reaches a page after it.
async fn stage_pre_truncate_insert(d: &Driver, key: &str, image: &str) {
    let mut client = d.pool().get().await.expect("pool");
    let txn = client.transaction().await.expect("begin");
    trellis::staging::append(
        &txn,
        &[StagedChange::Cdc {
            src_table: SRC.to_string(),
            key: key.to_string(),
            op: CdcOp::Insert,
            lsn: Some(PgLsn::from(1)),
            old_image: None,
            new_image: Some(image.to_string()),
            origin_lsn: None,
            src_changed: None,
            hop_gen: 0,
        }],
    )
    .await
    .expect("stage the pre-truncate change");
    txn.commit().await.expect("commit");
}

#[tokio::test]
async fn a_change_from_before_a_truncate_does_not_bring_back_a_one_to_one_row() {
    a_change_from_before_a_truncate_does_not_come_back(Flavour::OneToOne).await;
}

/// A page whose placeholder another page is queued behind deletes it,
/// and the queued page's insert then goes ahead. Page A holds a
/// change to key 9 from before the truncate, and is frozen once its insert
/// has given the key a placeholder; page B holds the key's real insert, and
/// its own insert of the entry queues on A's uncommitted one. A's statement
/// refuses its change and deletes the placeholder, so once A commits B's
/// insert finds no entry and goes ahead: B commits, the key's entry holds
/// B's change, and the key's row is in the target.
async fn a_dropped_placeholder_lets_a_queued_insert_write_the_entry(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10)]).await;
    write(&d, "truncate public.src").await;
    d.settle().await;
    stage_pre_truncate_insert(&d, "9", r#"{"id":"9","g":"1","v":"90"}"#).await;
    let stale = d.seal().await;
    write(&d, "insert into public.src values (9, 1, 90)").await;
    let live = d.seal().await;

    let mut page_a = d
        .drain_frozen(
            stale,
            "a",
            &[(PausePoint::AfterPlaceholders, flavour.target())],
        )
        .await;
    let frozen_a = page_a.reached(PausePoint::AfterPlaceholders).await;
    let page_b = d.drain_frozen(live, "b", &[]).await;
    d.wait_blocked_behind(frozen_a.backend_pid).await;
    d.release(&mut page_a, PausePoint::AfterPlaceholders).await;
    page_a.finish_result().await.expect("page A");
    page_b.finish_result().await.expect("page B");

    assert_eq!(
        d.rows(&format!(
            "select __from_key from {} where __applied_lsn is not null",
            flavour.ledger()
        ))
        .await,
        ["(9)"],
        "page B's insert wrote key 9's entry with its change"
    );
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn a_dropped_placeholder_lets_a_queued_insert_write_the_entry_one_to_one() {
    a_dropped_placeholder_lets_a_queued_insert_write_the_entry(Flavour::OneToOne).await;
}

/// A truncate and an insert in one source transaction. The truncate
/// floor refuses every change at or below the truncate's ring `lsn`, so the
/// insert applies only because its `lsn` is strictly above the truncate's.
/// Each trigger reads `pg_current_wal_insert_lsn()`, and the truncate's
/// trigger then writes its own ring row, which moves the insert position on
/// before the insert's trigger reads it.
async fn a_truncate_then_an_insert_in_one_transaction_keeps_the_insert(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    write(
        &d,
        "begin; truncate public.src; insert into public.src values (7, 1, 70); commit",
    )
    .await;
    let ring: Vec<String> = (0..4)
        .map(|slot| format!("select op, key, lsn from {DEFAULT_SCHEMA}.seg_{slot}"))
        .collect();
    assert_eq!(
        d.rows(&format!(
            "select t.lsn < i.lsn from ({ring}) t, ({ring}) i \
             where t.op = 'truncate' and i.op = 'insert' and i.key = '7'",
            ring = ring.join(" union all ")
        ))
        .await,
        ["(t)"],
        "the insert's lsn is strictly above the truncate's"
    );
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn a_truncate_then_an_insert_in_one_transaction_keeps_the_one_to_one_row() {
    a_truncate_then_an_insert_in_one_transaction_keeps_the_insert(Flavour::OneToOne).await;
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
async fn exp2_10_one_to_one() {
    exp2_10(Flavour::OneToOne).await;
}

// ------------------------------------------------------ superseded issues

/// The CDC text-format image of `public.src`'s row `id`, as the old go-live
/// enumeration attached it to a `Recompute`.
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

/// A `Recompute` for key 1 carrying key 1's image (as the old go-live
/// enumeration staged it) drains after key 1 is deleted, and before the
/// delete's own batch. Group 1 must end without key 1.
async fn recompute_before_delete_batch(flavour: Flavour) {
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
async fn recompute_before_delete_batch_one_to_one() {
    recompute_before_delete_batch(Flavour::OneToOne).await;
}

/// Key 1 is quarantined. It moves a → z (C1); a Re-derive of z counts
/// it there; it moves z → b (C2); C1, C2 and a `Recompute` of key 1 fold into
/// one record, which the drain parks. Released, the replay must leave z
/// without key 1.
async fn moved_key_released_from_quarantine(flavour: Flavour) {
    let mut d = start(flavour, A_Z_B).await;
    // Whole-key poison is per transform: key 1 is held for the one
    // definition on the source.
    d.ctl
        .execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, $1, '1', 'test' from transform_definitions \
             where target_table = $2",
            &[&SRC, &flavour.target()],
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
    let (_, target) = flavour.target().split_once('.').expect("qualified target");
    trellis::staging::release_key(d.pool(), target, SRC, "1")
        .await
        .expect("release key 1");
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn moved_key_released_from_quarantine_one_to_one() {
    moved_key_released_from_quarantine(Flavour::OneToOne).await;
}

// ------------------------------------------------------------ C's Q7

/// C's Q7: an application `AFTER ROW` trigger rewrites the row its
/// own statement wrote. The nested statement's capture runs first, so the
/// outer statement's ring row has the higher position. It
/// images the live row, the final one, not the transition table's.
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

/// Imaging the transition row instead would give `[(1,1,30), (2,3,5)]`
/// against the oracle's `[(1,2,30), (2,3,5)]`: a 1-1 Apply writes its image,
/// and the last captured image would be key 1 in group 1. Capture images the
/// live row.
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
/// It fails if a Re-derive marks as seen a change it didn't see:
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
/// re-derived) with its `applied_lsn` unchanged by either Re-derive, in
/// every flavour.
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
    assert_chunk_bases(&d, flavour, &w_in_xid, &w_between_xid).await;
    assert_eq!(
        applied_lsn(&d, flavour, "3").await,
        applied_before,
        "a Re-derive leaves the entry's applied_lsn alone (the D split's Q1)"
    );
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

/// Key `key`'s ledger `applied_lsn`, as text (`None` when unset).
async fn applied_lsn(d: &Driver, flavour: Flavour, key: &str) -> Option<String> {
    d.ctl
        .query_one(
            &format!(
                "select __applied_lsn::text from {} where __from_key = $1",
                flavour.ledger()
            ),
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
async fn assert_chunk_bases(d: &Driver, flavour: Flavour, w_in: &str, w_between: &str) {
    let rows = d
        .ctl
        .query(
            &format!(
                "select __from_key, __basis::text, \
                        pg_visible_in_snapshot($1::text::xid8, __basis), \
                        pg_visible_in_snapshot($2::text::xid8, __basis) \
                 from {} where __from_key = any($3) order by __from_key",
                flavour.ledger()
            ),
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
async fn chunked_read_exact_point_one_to_one() {
    chunked_read_exact_point(Flavour::OneToOne).await;
}

// ------------------------------------------------------ tombstone GC (D7)
//
// `trellis::staging::collect_tombstones` deletes a 1-1 ledger's tombstones
// at or below the contiguous drained prefix. Each scenario also fails under
// the `early_tombstone_gc` plant, which collects through the highest drained
// segment instead.

/// The keys of `flavour`'s ledger tombstones, as `(key)` rows.
async fn tombstones(d: &Driver, flavour: Flavour) -> Vec<String> {
    d.rows(&format!(
        "select __from_key from {} where __tombstone order by 1",
        flavour.ledger()
    ))
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
    assert_eq!(tombstones(&d, flavour).await, ["(1)"]);
    assert_eq!(d.collect_tombstones().await, 0, "the update's batch lags");
    assert_eq!(tombstones(&d, flavour).await, ["(1)"]);
    d.drain(b1, "b").await;
    assert_eq!(d.collect_tombstones().await, 1, "nothing lags any more");
    assert!(tombstones(&d, flavour).await.is_empty());
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn exp2_9_gc_waits_for_the_older_update_one_to_one() {
    exp2_9_gc_waits_for_the_older_update(Flavour::OneToOne).await;
}

/// [`exp2_9_gc_waits_for_the_older_update`] for a key the ledger has never
/// had: key 3's insert lags in batch 1 while its delete drains from batch
/// 2. The delete's Apply writes key 3's tombstone in the lock's insert,
/// and that insert's segment stamp must keep it past the
/// GC until the insert drains, or the insert applies to no entry and brings
/// key 3 back.
async fn a_new_keys_tombstone_waits_for_its_older_insert(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    write(&d, "insert into public.src values (3, 2, 5)").await;
    let b1 = d.seal().await;
    write(&d, "delete from public.src where id = 3").await;
    let b2 = d.seal().await;
    d.drain(b2, "a").await;
    assert_eq!(tombstones(&d, flavour).await, ["(3)"]);
    assert_eq!(d.collect_tombstones().await, 0, "the insert's batch lags");
    d.drain(b1, "b").await;
    assert_eq!(d.collect_tombstones().await, 1, "nothing lags any more");
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn a_new_keys_tombstone_waits_for_its_older_insert_one_to_one() {
    a_new_keys_tombstone_waits_for_its_older_insert(Flavour::OneToOne).await;
}

/// Key `id`'s entry on `flavour`'s ledger: whether it is a tombstone, and
/// its `applied_seg`.
async fn entry_stamp(d: &Driver, flavour: Flavour, id: i32) -> (bool, Option<i64>) {
    let row = d
        .ctl
        .query_one(
            &format!(
                "select __tombstone, __applied_seg from {} where __from_key = $1",
                flavour.ledger()
            ),
            &[&id.to_string()],
        )
        .await
        .expect("the key's entry");
    (row.get(0), row.get(1))
}

/// Only a tombstone carries `applied_seg`: a write that leaves an
/// entry live leaves its stamp alone, so an Apply to a live entry changes no
/// indexed column, and the GC's partial index can still key on the stamp.
/// The stamp a live entry keeps is stale, here an earlier tombstone's, kept
/// through the key's revival. Key 1 is deleted (batch 1) and stamped,
/// revived (2), updated (3) and deleted again (4). Batch 3 lags while 4
/// drains: the second delete must raise the stale stamp to its own batch,
/// or the GC, whose prefix is batch 2, collects the tombstone and the
/// update applies to a fresh entry, bringing key 1 back.
async fn a_revived_keys_tombstone_is_stamped_afresh(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    assert_eq!(
        entry_stamp(&d, flavour, 1).await,
        (false, None),
        "a live entry no write made a tombstone has no stamp"
    );
    write(&d, "delete from public.src where id = 1").await;
    let b1 = d.seal().await;
    d.drain(b1, "a").await;
    assert_eq!(entry_stamp(&d, flavour, 1).await, (true, Some(b1)));
    write(&d, "insert into public.src values (1, 2, 11)").await;
    let b2 = d.seal().await;
    d.drain(b2, "a").await;
    assert_eq!(
        entry_stamp(&d, flavour, 1).await,
        (false, Some(b1)),
        "the revival keeps the old stamp"
    );
    write(&d, "update public.src set v = 12 where id = 1").await;
    let b3 = d.seal().await;
    write(&d, "delete from public.src where id = 1").await;
    let b4 = d.seal().await;
    d.drain(b4, "a").await;
    assert_eq!(entry_stamp(&d, flavour, 1).await, (true, Some(b4)));
    assert_eq!(d.collect_tombstones().await, 0, "the update's batch lags");
    d.drain(b3, "b").await;
    assert_eq!(d.collect_tombstones().await, 1, "nothing lags any more");
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn a_revived_keys_tombstone_is_stamped_afresh_one_to_one() {
    a_revived_keys_tombstone_is_stamped_afresh(Flavour::OneToOne).await;
}

/// A Re-derive that leaves an entry live stamps nothing, and an Apply
/// that then deletes the key stamps only its own batch, though the entry's
/// `basis` saw a change in a later one. That is enough: the delete was
/// applied because the `basis` doesn't see it, so every change the `basis`
/// sees completed before the delete, in its batch or an earlier one.
///
/// Key 1's Re-derive (batch 1) drains after its update (batch 2) commits:
/// the entry stays live, unstamped, with a `basis` that sees the update. The
/// delete (batch 3) drains, applies and stamps 3. While batch 2 lags the GC
/// must keep the tombstone, or the update, which the entry refuses, applies
/// to a fresh entry.
async fn a_live_rederive_then_a_delete_stamps_the_deletes_batch(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    d.stage_recomputes(SRC, &["1"]).await;
    let b1 = d.seal().await;
    write(&d, "update public.src set v = 15 where id = 1").await;
    let b2 = d.seal().await;
    d.drain(b1, "r").await;
    assert_eq!(
        entry_stamp(&d, flavour, 1).await,
        (false, None),
        "a Re-derive that leaves the entry live stamps nothing"
    );
    write(&d, "delete from public.src where id = 1").await;
    let b3 = d.seal().await;
    d.drain(b3, "a").await;
    assert_eq!(entry_stamp(&d, flavour, 1).await, (true, Some(b3)));
    assert_eq!(d.collect_tombstones().await, 0, "the update's batch lags");
    d.drain(b2, "b").await;
    assert_eq!(d.collect_tombstones().await, 1, "nothing lags any more");
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn a_live_rederive_then_a_delete_stamps_the_deletes_batch_one_to_one() {
    a_live_rederive_then_a_delete_stamps_the_deletes_batch(Flavour::OneToOne).await;
}

/// A Re-derive that leaves a tombstone stamps the newest segment its read
/// sees, not its page's, also on an entry that was live and
/// unstamped. Key 1's Re-derive (batch 1) drains after the key's
/// update (2) and delete (3, still the active segment) commit: it reads no
/// row, so it writes a tombstone whose `basis` sees both. The delete is then refused. While
/// batch 2 lags, the GC, whose prefix is batch 1, must keep the tombstone,
/// or the update applies to a fresh entry and brings key 1 back.
async fn a_rederives_tombstone_outlives_the_changes_its_read_saw(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    d.stage_recomputes(SRC, &["1"]).await;
    let b1 = d.seal().await;
    write(&d, "update public.src set v = 15 where id = 1").await;
    let b2 = d.seal().await;
    write(&d, "delete from public.src where id = 1").await;
    d.drain(b1, "r").await;
    let b3 = d.seal().await;
    let (tombstone, stamp) = entry_stamp(&d, flavour, 1).await;
    assert!(tombstone);
    assert!(
        stamp >= Some(b3),
        "stamped {stamp:?}, below the delete's batch {b3}"
    );
    d.drain(b3, "a").await;
    assert_eq!(d.collect_tombstones().await, 0, "the update's batch lags");
    d.drain(b2, "b").await;
    assert_eq!(d.collect_tombstones().await, 1, "nothing lags any more");
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn a_rederives_tombstone_outlives_the_changes_its_read_saw_one_to_one() {
    a_rederives_tombstone_outlives_the_changes_its_read_saw(Flavour::OneToOne).await;
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
    assert_eq!(tombstones(&d, flavour).await, ["(1)", "(3)"]);
    assert_eq!(
        d.collect_tombstones().await,
        1,
        "only the batch below the lagging one is in the drained prefix"
    );
    assert_eq!(tombstones(&d, flavour).await, ["(1)"]);
    d.drain(b2, "b").await;
    assert_eq!(d.collect_tombstones().await, 1);
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn an_out_of_order_drain_holds_gc_back_one_to_one() {
    an_out_of_order_drain_holds_gc_back(Flavour::OneToOne).await;
}

/// GC under a concurrent re-insert of a deleted key: the re-insert's page is
/// frozen after its placeholder insert, which found key 1's tombstone and
/// so inserted nothing. The GC collects that tombstone before the page's
/// entry lock, which then finds no entry for key 1: the page must roll
/// back and retry, inserting key 1's placeholder afresh, or the
/// re-insert is lost. The same page holds
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
    assert_eq!(tombstones(&d, flavour).await, ["(1)", "(3)"]);
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
    assert_eq!(tombstones(&d, flavour).await, ["(3)"]);
    d.release(&mut reinsert, PausePoint::AfterPlaceholders)
        .await;
    reinsert.finish().await;
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn gc_under_a_concurrent_reinsert_one_to_one() {
    gc_under_a_concurrent_reinsert(Flavour::OneToOne).await;
}

/// A page whose entry lock loses a key to the GC keeps I5's one global
/// order. Page A (batch 2) is frozen after its placeholder insert,
/// which found key 2's tombstone and so inserted nothing. The GC collects
/// that tombstone, and page B (batch 3, on the same keys) inserts key 2's
/// placeholder in its place and is frozen there. A then locks key 1, finds
/// no entry for key 2, rolls back and retries, and its retry's placeholder
/// insert for key 2 queues on B's uncommitted one. Released, B locks key 1.
/// Had A kept key 1's lock while inserting key 2's placeholder again, that
/// is a deadlock; A gives it back first, so B goes first and A follows.
async fn an_entry_lost_to_the_gc_never_deadlocks(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20), (3, 2, 5)]).await;
    write(&d, "delete from public.src where id = 2").await;
    let b1 = d.seal().await;
    d.drain(b1, "a").await;
    write(
        &d,
        "update public.src set v = 11 where id = 1; insert into public.src values (2, 1, 25)",
    )
    .await;
    let b2 = d.seal().await;
    write(
        &d,
        "update public.src set v = 12 where id = 1; update public.src set v = 26 where id = 2",
    )
    .await;
    let b3 = d.seal().await;
    assert_eq!(tombstones(&d, flavour).await, ["(2)"]);

    let mut page_a = d
        .drain_frozen(
            b2,
            "a",
            &[(PausePoint::AfterPlaceholders, flavour.target())],
        )
        .await;
    page_a.reached(PausePoint::AfterPlaceholders).await;
    assert_eq!(d.collect_tombstones().await, 1, "key 2's tombstone goes");
    let mut page_b = d
        .drain_frozen(
            b3,
            "b",
            &[(PausePoint::AfterPlaceholders, flavour.target())],
        )
        .await;
    let frozen_b = page_b.reached(PausePoint::AfterPlaceholders).await;
    d.release(&mut page_a, PausePoint::AfterPlaceholders).await;
    d.wait_blocked_behind(frozen_b.backend_pid).await;
    d.release(&mut page_b, PausePoint::AfterPlaceholders).await;
    let (a, b) = tokio::join!(page_a.finish_result(), page_b.finish_result());

    assert_eq!(d.deadlocks_logged(), Vec::<String>::new());
    a.expect("page A");
    b.expect("page B");
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn an_entry_lost_to_the_gc_never_deadlocks_one_to_one() {
    an_entry_lost_to_the_gc_never_deadlocks(Flavour::OneToOne).await;
}

/// A GC batch reads the ledger through its tombstone index, never by a
/// sequential or bitmap scan, so its cost is the tombstones it
/// collects, not the ledger's size, and the index keys on
/// `__applied_seg`, so it isn't the tombstones not yet collectable either.
/// Each load is analyzed to tempt the
/// planner away from the index:
///
/// - tombstone-heavy (a quarter of the entries, all at or below the
///   prefix): left to itself the planner seq-scans, since a scan with the
///   batch's `limit` looks cheap. After the GC has taken them it is the
///   whole ledger, which is what held `xmin` for 33 s at 100M;
/// - sparse (fewer collectable tombstones than a batch takes): with only
///   sequential scans off it builds a bitmap of every one of them before the
///   `limit` applies.
async fn the_gc_statement_reads_the_ledger_by_its_tombstone_index(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10)]).await;
    let ledger = flavour.ledger();
    let index: String = d
        .ctl
        .query_one(
            "select indexrelid::regclass::text from pg_index \
             where indrelid = $1::text::regclass \
               and pg_get_expr(indpred, indrelid) = '__tombstone'",
            &[&ledger],
        )
        .await
        .expect("the ledger's tombstone index")
        .get(0);
    let index = index.rsplit('.').next().expect("an index name").to_string();
    for (load, seg, tombstone) in [
        ("tombstone-heavy", "1 + i % 10", "i % 4 = 0"),
        ("sparse", "1 + (i * 7919) % 10", "i % 50 = 0"),
    ] {
        d.ctl
            .batch_execute(&format!(
                "truncate {ledger}; \
                 insert into {ledger} (__from_key, __applied_seg, __tombstone) \
                 select 'x' || i, {seg}, {tombstone} from generate_series(1, 20000) i; \
                 analyze {ledger}"
            ))
            .await
            .expect("load and analyze the ledger");
        let explained = trellis::staging::retire::explain_collect(&mut d.ctl, ledger, 10)
            .await
            .expect("explain the GC statement");
        let scans: Vec<&str> = explained
            .lines()
            .filter(|line| line.contains("Seq Scan") || line.contains("Bitmap"))
            .collect();
        assert_eq!(
            scans,
            Vec::<&str>::new(),
            "{load}: no sequential or bitmap scan:\n{explained}"
        );
        assert!(
            explained.contains(&format!("Index Scan using {index}")),
            "{load}: the batch walks {index}:\n{explained}"
        );
        // It seeks the collectable tombstones by their stamp, so its cost
        // doesn't grow with the tombstones above the drained prefix.
        assert!(
            explained
                .lines()
                .any(|line| line.contains("Index Cond:") && line.contains("__applied_seg")),
            "{load}: the batch seeks __applied_seg:\n{explained}"
        );
        // The inner select locks its rows, so the index's predicate stays a
        // filter: read committed re-checks it on a row updated under the
        // batch, which a page reviving the entry does.
        assert!(
            explained
                .lines()
                .any(|line| line.contains("Filter:") && line.contains("__tombstone")),
            "{load}: the batch re-checks __tombstone:\n{explained}"
        );
    }
}

#[tokio::test]
async fn the_gc_statement_reads_the_ledger_by_its_tombstone_index_one_to_one() {
    the_gc_statement_reads_the_ledger_by_its_tombstone_index(Flavour::OneToOne).await;
}

/// The page's statements that read a ledger by entry key reach the entries
/// through the key's index while the ledger's statistics lag its growth:
/// never analyzed (`empty`), or analyzed while it held a handful of
/// entries (`stale`), and then grown by a large insert, as a fast-filling
/// ledger is before `autoanalyze` catches up. Left to the planner, the
/// page's entry update joined its records to a sequential scan of the
/// whole ledger, which cost 200–345 ms a page in the paged benchmark.
async fn the_page_statements_read_the_ledger_by_key(flavour: Flavour) {
    let mut unbounded = Vec::new();
    for stats in ["empty", "stale"] {
        let d = start(flavour, &[]).await;
        let ledger = flavour.ledger();
        let ledger_name = ledger.rsplit('.').next().expect("a table name");
        let (columns, values) = ("__from_key", "i::text");
        let load = |from: i32, to: i32| {
            format!(
                "insert into {ledger} ({columns}) \
                 select {values} from generate_series({from}, {to}) i"
            )
        };
        d.ctl
            .batch_execute(&format!(
                "alter table {ledger} set (autovacuum_enabled = false)"
            ))
            .await
            .expect("keep autoanalyze off the ledger");
        if stats == "stale" {
            d.ctl
                .batch_execute(&format!("{}; analyze {ledger}", load(1, 100)))
                .await
                .expect("load and analyze a small ledger");
        }
        let from = if stats == "stale" { 101 } else { 1 };
        d.ctl
            .batch_execute(&load(from, 40_000))
            .await
            .expect("grow the ledger");
        let keys: Vec<String> = (1..=500).map(|i| (i * 79).to_string()).collect();
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        let plans =
            trellis::staging::explain_one_to_one_ledger_page(d.pool(), flavour.target(), &keys)
                .await
                .expect("explain the page's statements");
        for (statement, explained) in plans {
            // Every read of the ledger is bounded by the page's keys: no
            // sequential scan, and no index scan over the whole ledger
            // feeding a hash join, which `enable_seqscan = off` alone
            // leaves the planner.
            let bounded = !explained.lines().any(|line| {
                line.contains("Scan")
                    && line.contains(&format!(" on {ledger_name} "))
                    && (line.contains("Seq Scan") || estimated_rows(line) > keys.len())
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
        "every read of the ledger is by the page's keys:\n{}",
        unbounded.join("\n")
    );
}

/// The row estimate of an `explain` line's node: its `rows=`.
fn estimated_rows(line: &str) -> usize {
    line.split("rows=")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .and_then(|rows| rows.parse().ok())
        .unwrap_or(0)
}

#[tokio::test]
async fn the_page_statements_read_the_ledger_by_key_one_to_one() {
    the_page_statements_read_the_ledger_by_key(Flavour::OneToOne).await;
}

/// Whether key `id`'s entry on `flavour`'s ledger is a heap-only tuple: the
/// new version of a HOT update, which wrote no index entry.
async fn heap_only(d: &Driver, flavour: Flavour, id: i32) -> bool {
    let ledger = flavour.ledger();
    d.ctl
        .batch_execute("create extension if not exists pageinspect")
        .await
        .expect("pageinspect");
    d.ctl
        .query_one(
            &format!(
                "select (i.t_infomask2 & 32768) <> 0 \
                 from {ledger} l, \
                      heap_page_items(get_raw_page('{ledger}', (l.ctid::text::point)[0]::int)) i \
                 where l.__from_key = $1 and i.lp = (l.ctid::text::point)[1]::int"
            ),
            &[&id.to_string()],
        )
        .await
        .expect("the entry's tuple")
        .get(0)
}

/// An Apply or a Re-derive that leaves an entry live, in its group, changes
/// no column a ledger index reads, so it can be a HOT update and write no
/// index entry. It moves `applied_lsn` or `basis` and the entry's
/// contributions, which no index reads, and leaves `applied_seg`, which the
/// GC's partial index keys on, alone: only a tombstone carries it
/// (`defs::ledger::tombstone_seg_sql`). Key 1 is updated (an Apply) and
/// then re-derived, each in a page of its own, and both times its entry's
/// new version is heap-only. Its stamp, stale from an earlier tombstone, is
/// left as it was.
async fn a_live_entrys_apply_and_rederive_are_hot(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10), (2, 1, 20)]).await;
    write(&d, "delete from public.src where id = 1").await;
    let b1 = d.seal().await;
    d.drain(b1, "a").await;
    write(&d, "insert into public.src values (1, 1, 11)").await;
    let b2 = d.seal().await;
    d.drain(b2, "a").await;
    assert_eq!(entry_stamp(&d, flavour, 1).await, (false, Some(b1)));

    write(&d, "update public.src set v = 12 where id = 1").await;
    let b3 = d.seal().await;
    d.drain(b3, "a").await;
    assert!(heap_only(&d, flavour, 1).await, "the Apply is HOT");
    d.stage_recomputes(SRC, &["1"]).await;
    let b4 = d.seal().await;
    d.drain(b4, "a").await;
    assert!(heap_only(&d, flavour, 1).await, "the Re-derive is HOT");
    assert_eq!(entry_stamp(&d, flavour, 1).await, (false, Some(b1)));
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn a_live_entrys_apply_and_rederive_are_hot_one_to_one() {
    a_live_entrys_apply_and_rederive_are_hot(Flavour::OneToOne).await;
}

/// A page's Apply of a key with no entry writes the entry in its lock's
/// insert, and the insert is the lock (I1): a Re-derive of the same
/// key queues on that uncommitted entry and reads only once it commits.
/// Page A (key 2's insert) is frozen holding the entry it wrote; key 2 is
/// then moved to group 2, and page R, the move folded with a Re-derive of
/// key 2, queues behind A. Released, R reads the moved row and moves key 2
/// out of the group A counted it in. Had R not waited, it would find no
/// entry and count key 2 a second time beside A's.
async fn a_rederive_queues_on_a_new_keys_applied_entry(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10)]).await;
    write(&d, "insert into public.src values (2, 1, 5)").await;
    let insert = d.seal().await;
    let mut a = d
        .drain_frozen(
            insert,
            "a",
            &[(PausePoint::AfterEntryLock, flavour.target())],
        )
        .await;
    let frozen = a.reached(PausePoint::AfterEntryLock).await;
    write(&d, "update public.src set g = 2, v = 50 where id = 2").await;
    d.stage_recomputes(SRC, &["2"]).await;
    let rederive = d.seal().await;
    let r = d.drain_frozen(rederive, "r", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut a, PausePoint::AfterEntryLock).await;
    a.finish().await;
    r.finish().await;
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn a_rederive_queues_on_a_new_keys_applied_entry_one_to_one() {
    a_rederive_queues_on_a_new_keys_applied_entry(Flavour::OneToOne).await;
}

/// The other order: a Re-derive's placeholder for a key with no entry holds
/// off a page whose Apply would write that entry in its insert. Page
/// R, a Re-derive of key 2, is frozen after its placeholder insert, before
/// its read; key 2's insert and then an update commit; page A, draining
/// both (folded into an Apply of the update), queues on R's placeholder.
/// Released, R reads the updated row. A's insert then finds R's entry, so
/// A writes nothing in it and goes through the lock and I2 like any other
/// key: the update is visible in R's basis and skipped. Had A taken key 2
/// for one its insert wrote, it would count R's entry in again.
async fn a_new_keys_apply_queues_on_a_rederives_placeholder(flavour: Flavour) {
    let mut d = start(flavour, &[(1, 1, 10)]).await;
    d.stage_recomputes(SRC, &["2"]).await;
    let rederive = d.seal().await;
    let mut r = d
        .drain_frozen(
            rederive,
            "r",
            &[(PausePoint::AfterPlaceholders, flavour.target())],
        )
        .await;
    let frozen = r.reached(PausePoint::AfterPlaceholders).await;
    write(&d, "insert into public.src values (2, 1, 5)").await;
    write(&d, "update public.src set v = 50 where id = 2").await;
    let writes = d.seal().await;
    let a = d.drain_frozen(writes, "a", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut r, PausePoint::AfterPlaceholders).await;
    r.finish().await;
    a.finish().await;
    assert_oracle(&mut d, flavour).await;
}

#[tokio::test]
async fn a_new_keys_apply_queues_on_a_rederives_placeholder_one_to_one() {
    a_new_keys_apply_queues_on_a_rederives_placeholder(Flavour::OneToOne).await;
}

// ------------------------------------- a 1-1 and an aggregate in one page

// A column resume's and an `ALTER TRANSFORM`'s rewrite of a 1-1 field are
// field builds; their chunks' interleavings with pages are in
// `one_to_one_build_interleavings.rs` ("a field build's chunk").

// ---------------------------------- fresh and read entries in one page
