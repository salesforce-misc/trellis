//! `MIN`/`MAX` on a plain group-basis aggregate, kept by recompute on
//! retract (#1117's design R, rules Rx1–Rx8).
//!
//! Each group's hidden row holds its extremes and their own basis
//! (`__xbasis`) and stamp (`__xstamp`), apart from the partials' basis. An
//! insertion moves an extreme by `least`/`greatest`; a retraction that
//! reaches it, or any retraction while it is `NULL`, makes the page re-read
//! the group's extremes from the source with one probe, after its lock, and
//! the probe's snapshot becomes the extremes' basis.
//!
//! The tests port the research's deterministic families: F2 (transaction
//! sequences drained out of order), F4 (changes racing the probe),
//! F4p (a change committed and drained between a page's staging and its
//! lock), F5 (`NULL` arguments and `NULL` group keys) and F6 (the zero-group
//! GC after a probe); F9 (partitioned sources) is in `partition_group_basis`.
//! Each drives its interleaving by hand through `support/drain_driver.rs`,
//! with pause points, and none sleeps or polls for convergence. Every source
//! starts empty and is seeded through capture, and is defined through the
//! real validator, with the probe's index define requires.
//!
//! Each family names the necessity mutation it fails under: `NOBASIS` (the
//! probe writes no `__xbasis`), `SHARED` (the probe stamps the partials'
//! basis instead of its own), `PRELOCK` (the probe reads before the lock
//! statement), `NULL` (a `NULL` extreme never probes), `NULLKEY` (the probe
//! matches a `NULL` group key by `=`) and `GCX` (the GC ignores `__xstamp`).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{PgType, ValueType};
use trellis::float::FloatWidth;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;
use trellis::staging::self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};

const SRC: &str = "public.src";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g \
                   SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n";
const SOURCE_DDL: &str = "create table public.src (id integer primary key, g integer, v integer); \
                          create index on public.src (g, v)";
const ACTUAL: &str = "select g, lo, hi, n from public.agg order by g";
const EXPECTED: &str = "select g, min(v), max(v), count(*) from public.src group by g order by g";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// `public.src (id, g, v)`, empty, with `definition` live over it on the
/// group basis, captured by triggers, its hidden table keeping extremes.
async fn start_with(definition: &str, source_ddl: &str, columns: &[(&str, ValueType)]) -> Driver {
    let d = Driver::start(source_ddl, columns, &[definition], &[SRC]).await;
    assert_eq!(
        scalar(
            &d,
            &format!(
                "select count(*) from pg_attribute a join pg_class c on c.oid = a.attrelid \
                 join pg_namespace n on n.oid = c.relnamespace \
                 where n.nspname = '{DEFAULT_SCHEMA}' and c.relname like 'agg\\_%' \
                   and a.attname in ('__xbasis', '__xstamp')"
            ),
        )
        .await,
        "2",
        "the definition is on the group basis, with its extremes' basis"
    );
    d
}

async fn start() -> Driver {
    start_with(
        AGG,
        SOURCE_DDL,
        &[("id", int4()), ("g", int4()), ("v", int4())],
    )
    .await
}

/// `sql`'s one value, as text.
async fn scalar(d: &Driver, sql: &str) -> String {
    let rows = d.rows(sql).await;
    assert_eq!(rows.len(), 1, "{sql}: {rows:?}");
    rows[0].trim_matches(|c| c == '(' || c == ')').to_string()
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it as one sealed page.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

/// The target equals the oracle now, before anything else drains.
async fn assert_now(d: &Driver, actual: &str, expected: &str, what: &str) {
    assert_eq!(
        d.rows(actual).await,
        d.rows(expected).await,
        "{what}: the target (left) differs from the oracle (right)"
    );
}

/// Settles the pipeline and asserts the target equals the oracle.
async fn assert_oracle(d: &mut Driver) {
    d.settle().await;
    assert_now(d, ACTUAL, EXPECTED, "settled").await;
}

/// The hidden table of the one definition, qualified.
async fn hidden(d: &Driver) -> String {
    let id = scalar(
        d,
        "select id from transform_definitions where target_table = 'public.agg'",
    )
    .await;
    format!("{DEFAULT_SCHEMA}.agg_{id}")
}

/// Group `g`'s hidden extremes: `(min, max, has the extremes' basis)`.
async fn extremes(d: &Driver, g: i32) -> Vec<String> {
    d.rows(&format!(
        "select __min_1, __max_1, __xbasis is not null from {} where g = {g}",
        hidden(d).await
    ))
    .await
}

// ------------------------------------------------------------------ pages

/// Inserts, updates that move the extreme in and out of a group, `NULL`
/// arguments, a `NULL` group key and deletes, each its own page, against
/// Postgres over the source. A delete of the minimum probes; a delete of a
/// middle value doesn't.
#[tokio::test]
async fn pages_keep_min_and_max_exact() {
    let mut d = start().await;
    for sql in [
        "insert into public.src values (1, 1, 10), (2, 1, 5), (3, 2, null), (4, null, 7)",
        "update public.src set v = 20 where id = 1",
        "update public.src set g = 2 where id = 2",
        "update public.src set v = null where id = 1; insert into public.src values (5, 3, 1)",
        "insert into public.src values (6, 2, 3), (7, 2, 4), (8, 2, 9)",
        "delete from public.src where id = 7",
        "delete from public.src where id = 6",
        "delete from public.src where id = 8",
        "delete from public.src where id in (3, 4)",
        "update public.src set g = null where id = 5",
        "delete from public.src",
    ] {
        step(&mut d, sql).await;
        assert_now(&d, ACTUAL, EXPECTED, sql).await;
    }
    assert_oracle(&mut d).await;
}

/// A page that retracts no extreme reads no source row: its extremes keep
/// no basis. One that retracts the minimum probes, and the probe's
/// snapshot becomes the extremes' basis.
#[tokio::test]
async fn only_a_retraction_reaching_an_extreme_probes() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 1), (2, 1, 5), (3, 1, 9)",
    )
    .await;
    step(&mut d, "delete from public.src where id = 2").await;
    assert_eq!(extremes(&d, 1).await, ["(1,9,f)"]);
    step(&mut d, "delete from public.src where id = 1").await;
    assert_eq!(extremes(&d, 1).await, ["(9,9,t)"]);
    assert_oracle(&mut d).await;
}

/// Rx6, the build: a definition over a source that already has rows is
/// built by one absolute write of every group, which writes the extremes
/// and their basis from the snapshot it writes the partials from. Pages
/// after it apply against both bases.
#[tokio::test]
async fn a_build_writes_the_extremes_and_their_basis() {
    let mut d = Driver::start(
        &format!(
            "{SOURCE_DDL}; insert into public.src values \
             (1, 1, 10), (2, 1, 5), (3, 2, null), (4, null, 7), (5, null, 2)"
        ),
        &[("id", int4()), ("g", int4()), ("v", int4())],
        &[AGG],
        &[SRC],
    )
    .await;
    assert_now(&d, ACTUAL, EXPECTED, "built").await;
    assert_eq!(extremes(&d, 1).await, ["(5,10,t)"]);
    assert_eq!(extremes(&d, 2).await, ["(,,t)"]);
    for sql in [
        "delete from public.src where id = 2",
        "delete from public.src where id = 5",
        "update public.src set v = 1 where id = 3",
    ] {
        step(&mut d, sql).await;
        assert_now(&d, ACTUAL, EXPECTED, sql).await;
    }
    assert_oracle(&mut d).await;
}

// ----------------------------------------------------------------------- F2

/// One F2 transaction sequence on its own group `g`: the rows it seeds
/// (`(id, v)`, in group `g`), then up to three transactions, each written
/// into its own sealed batch. `{g}` in a transaction is the group, `{o}`
/// another group of its own (`g + 100`), `{i}` an id base (`g * 10`).
struct Sequence {
    name: &'static str,
    seed: &'static [(i32, Option<i32>)],
    txns: [&'static str; 3],
}

const SEQUENCES: &[Sequence] = &[
    Sequence {
        name: "A.ins-low-del",
        seed: &[(0, Some(10))],
        txns: [
            "insert into public.src values ({i} + 1, {g}, 3)",
            "delete from public.src where id = {i} + 1",
            "",
        ],
    },
    Sequence {
        name: "B.ins-high-del",
        seed: &[(0, Some(10))],
        txns: [
            "insert into public.src values ({i} + 1, {g}, 30)",
            "delete from public.src where id = {i} + 1",
            "",
        ],
    },
    Sequence {
        name: "C.dip-then-up",
        seed: &[(0, Some(10)), (1, Some(12))],
        txns: [
            "update public.src set v = 2 where id = {i}",
            "update public.src set v = 20 where id = {i}",
            "",
        ],
    },
    Sequence {
        name: "D.ins-upd-del",
        seed: &[(0, Some(10))],
        txns: [
            "insert into public.src values ({i} + 1, {g}, 5)",
            "update public.src set v = 15 where id = {i} + 1",
            "delete from public.src where id = {i} + 1",
        ],
    },
    Sequence {
        name: "E.min-out-and-back",
        seed: &[(0, Some(1)), (1, Some(10))],
        txns: [
            "update public.src set g = {o} where id = {i}",
            "update public.src set g = {g} where id = {i}",
            "",
        ],
    },
    Sequence {
        name: "F.min-del-reins",
        seed: &[(0, Some(1)), (1, Some(10))],
        txns: [
            "delete from public.src where id = {i}",
            "insert into public.src values ({i}, {g}, 1)",
            "",
        ],
    },
    Sequence {
        name: "G.group-emptied",
        seed: &[(0, Some(5))],
        txns: [
            "delete from public.src where id = {i}",
            "insert into public.src values ({i} + 1, {g}, 7)",
            "delete from public.src where id = {i} + 1",
        ],
    },
    Sequence {
        name: "H.min-null-and-back",
        seed: &[(0, Some(1)), (1, Some(10))],
        txns: [
            "update public.src set v = null where id = {i}",
            "update public.src set v = 1 where id = {i}",
            "",
        ],
    },
    Sequence {
        name: "I.lower-in-then-del",
        seed: &[(0, Some(10))],
        txns: [
            "insert into public.src values ({i} + 1, {o}, 2)",
            "update public.src set g = {g} where id = {i} + 1",
            "delete from public.src where id = {i} + 1",
        ],
    },
    Sequence {
        name: "J.tie-del-del",
        seed: &[(0, Some(3)), (1, Some(3)), (2, Some(10))],
        txns: [
            "delete from public.src where id = {i}",
            "delete from public.src where id = {i} + 1",
            "",
        ],
    },
];

/// `sql` for the sequence on group `g`.
fn bind(sql: &str, g: i32) -> String {
    sql.replace("{g}", &g.to_string())
        .replace("{o}", &(g + 100).to_string())
        .replace("{i}", &(g * 10).to_string())
}

/// Runs every [`SEQUENCES`] entry, each on its own group, by `script`: `wN`
/// writes every sequence's transaction `N`, `s` seals a batch, `dN` drains
/// the `N`th batch sealed, and `r` stages an image-less change for every key
/// the sequences touch (a page's group recompute, Rx6, at that point).
async fn f2_run(script: &str) {
    f2_run_checked(script, false).await;
}

/// [`f2_run`], and with `checked`, a comparison of every page size after
/// each action of `script`, with whatever the ring holds: it must report no
/// divergence on a group it is certain of.
async fn f2_run_checked(script: &str, checked: bool) {
    let mut d = start().await;
    let mut seed = Vec::new();
    for (k, sequence) in SEQUENCES.iter().enumerate() {
        let g = k as i32 + 1;
        for (id, v) in sequence.seed {
            let v = v.map_or("null".to_string(), |v| v.to_string());
            seed.push(format!("({}, {g}, {v})", g * 10 + id));
        }
    }
    step(
        &mut d,
        &format!("insert into public.src values {}", seed.join(", ")),
    )
    .await;
    let mut batches = Vec::new();
    for action in script.split_whitespace() {
        let (verb, n) = action.split_at(1);
        match verb {
            "w" => {
                let t: usize = n.parse().expect("a transaction");
                for (k, sequence) in SEQUENCES.iter().enumerate() {
                    if !sequence.txns[t].is_empty() {
                        write(&d, &bind(sequence.txns[t], k as i32 + 1)).await;
                    }
                }
            }
            "s" => batches.push(d.seal().await),
            "d" => {
                let b: usize = n.parse().expect("a batch");
                d.drain(batches[b], action).await;
            }
            "r" => {
                let keys: Vec<String> = (1..=SEQUENCES.len() as i32)
                    .flat_map(|g| (0..3).map(move |id| (g * 10 + id).to_string()))
                    .collect();
                let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
                d.stage_recomputes(SRC, &keys).await;
            }
            _ => panic!("{action}"),
        }
        if checked {
            let mut bad = Vec::new();
            for limit in [1, 2, 100] {
                let found = compare_certain(&d, limit).await;
                if !found.is_empty() {
                    bad.push((limit, found));
                }
            }
            assert_eq!(bad, vec![], "{script}: after {action}");
        }
    }
    let names: Vec<&str> = SEQUENCES.iter().map(|s| s.name).collect();
    assert_now(
        &d,
        ACTUAL,
        EXPECTED,
        &format!("{script}: sequences {names:?} on groups 1.."),
    )
    .await;
    assert_oracle(&mut d).await;
}

/// The page orders F2 runs. The ring holds the first batch until it drains
/// (a seal reuses its slot), so a script that seals three batches drains
/// the first before the third. The orders that would drain the third or
/// the second before the first put the later transactions on one page
/// instead: T2 and T3 on one page before T1's, T3 before a page of T1 and
/// T2.
const ORDERS: [&str; 5] = [
    "w0 s w1 s d0 w2 s d1 d2",
    "w0 s w1 s d0 w2 s d2 d1",
    "w0 s w1 s d1 d0 w2 s d2",
    "w0 s w1 w2 s d1 d0",
    "w0 w1 s w2 s d1 d0",
];

/// F2: every sequence, its pages drained in each of [`ORDERS`]. A page that
/// applies a retraction before the insertion it retracts probes (the
/// retraction reaches the extreme, or the extreme is `NULL`), and the
/// probe's basis refuses the insertion when its page drains. Fails under
/// `NOBASIS` (the late insertion moves the extreme back) and `SHARED` (the
/// probe's basis refuses the late insertion's count).
#[tokio::test]
async fn f2_every_sequence_drained_out_of_order() {
    for order in ORDERS {
        f2_run(order).await;
    }
}

/// F2 with a group recompute between the commits: the recompute writes the
/// extremes and their basis with the partials (Rx6), and the pages after it
/// apply against both bases.
#[tokio::test]
async fn f2_every_sequence_drained_out_of_order_with_a_recompute() {
    for order in ORDERS {
        f2_run(&order.replacen("w0", "w0 r", 1)).await;
    }
}

/// `self_check` agrees with the target at every step of every F2 order,
/// with and without a recompute: whatever the ring holds, whichever pages
/// drained and whatever their probes saw, a comparison of the extremes
/// under `__xbasis` (and of the partials under `__basis`) reports no group
/// it is certain of. Fails if the extremes are read under `__basis`, or if
/// a subtracted image's value stays in the extremes.
#[tokio::test]
async fn self_check_agrees_at_every_step_of_f2() {
    for order in ORDERS {
        f2_run_checked(order, true).await;
        f2_run_checked(&order.replacen("w0", "w0 r", 1), true).await;
    }
}

// ----------------------------------------------------------------------- F4

/// F4, a racer committed while the page holds group 1 at `point`: the page
/// deletes the minimum (1), and the racer, `racer`, commits before the
/// page's probe. The probe sees it, and its basis refuses it for the
/// extremes when its own page drains; its counts still apply. Fails under
/// `SHARED`: the probe's snapshot, stamped as the partials' basis, refuses
/// the racer's count.
async fn f4_racer(point: PausePoint, racer: &str, want: &str) {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 1), (2, 1, 5), (3, 1, 9), (4, 2, 2)",
    )
    .await;
    write(&d, "delete from public.src where id = 1").await;
    let batch = d.seal().await;
    let mut page = d.drain_frozen(batch, "a", &[(point, "public.agg")]).await;
    page.reached(point).await;
    write(&d, racer).await;
    d.release(&mut page, point).await;
    page.finish().await;
    let probed = extremes(&d, 1).await;
    let batch = d.seal().await;
    d.drain(batch, "b").await;
    assert_now(&d, ACTUAL, EXPECTED, &format!("{point:?}: {racer}")).await;
    assert_eq!(
        probed,
        [want],
        "{point:?}: {racer}: the probe saw the racer"
    );
    assert_oracle(&mut d).await;
}

#[tokio::test]
async fn f4_an_insert_below_the_next_value_racing_the_probe() {
    for point in [PausePoint::AfterEntryLock, PausePoint::BeforeExtremeProbe] {
        f4_racer(point, "insert into public.src values (5, 1, 3)", "(3,9,t)").await;
    }
}

#[tokio::test]
async fn f4_a_lower_value_moved_in_racing_the_probe() {
    for point in [PausePoint::AfterEntryLock, PausePoint::BeforeExtremeProbe] {
        f4_racer(point, "update public.src set g = 1 where id = 4", "(2,9,t)").await;
    }
}

#[tokio::test]
async fn f4_a_delete_of_the_next_value_racing_the_probe() {
    for point in [PausePoint::AfterEntryLock, PausePoint::BeforeExtremeProbe] {
        f4_racer(point, "delete from public.src where id = 2", "(9,9,t)").await;
    }
}

/// F4, two pages on one group: the racer's page drains while the probing
/// page holds group 1 frozen before its probe, and queues on the group's
/// lock. Once the first commits, the second applies against the extremes'
/// new basis, which saw the racer, and against the partials' basis, which
/// didn't. Fails under `SHARED`.
#[tokio::test]
async fn f4_a_page_on_the_group_queues_behind_the_probing_page() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 1), (2, 1, 5), (3, 1, 9)",
    )
    .await;
    write(&d, "delete from public.src where id = 1").await;
    let first = d.seal().await;
    let mut page = d
        .drain_frozen(
            first,
            "a",
            &[(PausePoint::BeforeExtremeProbe, "public.agg")],
        )
        .await;
    let frozen = page.reached(PausePoint::BeforeExtremeProbe).await;
    write(&d, "insert into public.src values (4, 1, 3)").await;
    let second = d.seal().await;
    let racer = d.drain_frozen(second, "b", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut page, PausePoint::BeforeExtremeProbe).await;
    page.finish().await;
    racer.finish().await;
    assert_now(&d, ACTUAL, EXPECTED, "after both pages").await;
    assert_eq!(extremes(&d, 1).await, ["(3,9,t)"]);
    assert_oracle(&mut d).await;
}

// ---------------------------------------------------------------------- F4p

/// F4p (`del-next@prelock`): the page deleting the minimum (1) has staged
/// its sides and not yet locked group 1 when the next value (5) is deleted,
/// and that change's page drains and commits: it retracts 5, which doesn't
/// reach the minimum, so it doesn't probe. The first page then locks, and
/// its probe, after the lock, reads 9. Fails under `PRELOCK`: a probe read
/// before the lock statement sees 5 and keeps it.
#[tokio::test]
async fn f4p_the_next_value_deleted_and_drained_before_the_lock() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 1), (2, 1, 5), (3, 1, 9)",
    )
    .await;
    write(&d, "delete from public.src where id = 1").await;
    let first = d.seal().await;
    let mut page = d
        .drain_frozen(first, "a", &[(PausePoint::BeforeGroupLock, "public.agg")])
        .await;
    page.reached(PausePoint::BeforeGroupLock).await;
    write(&d, "delete from public.src where id = 2").await;
    let second = d.seal().await;
    d.drain(second, "b").await;
    assert_eq!(extremes(&d, 1).await, ["(1,9,f)"], "the racer didn't probe");
    d.release(&mut page, PausePoint::BeforeGroupLock).await;
    page.finish().await;
    assert_now(&d, ACTUAL, EXPECTED, "both pages").await;
    assert_eq!(extremes(&d, 1).await, ["(9,9,t)"]);
    assert_oracle(&mut d).await;
}

// ----------------------------------------------------------------------- F5

/// F5 (`allnull-value-back.rev`): a group whose only argument is `NULL`
/// gains a value (T1) and loses it again (T2), and T2's page drains first.
/// Its retraction finds the minimum `NULL` and probes, so T1's insertion is
/// refused when it drains. Fails under `NULL`: a `NULL` extreme doesn't
/// probe, and T1 then leaves 5 behind.
#[tokio::test]
async fn f5_an_all_null_group_gains_and_loses_a_value_in_reverse() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, null), (2, 1, null)",
    )
    .await;
    assert_eq!(extremes(&d, 1).await, ["(,,f)"]);
    write(&d, "update public.src set v = 5 where id = 1").await;
    let t1 = d.seal().await;
    write(&d, "update public.src set v = null where id = 1").await;
    let t2 = d.seal().await;
    d.drain(t2, "a").await;
    d.drain(t1, "b").await;
    assert_now(&d, ACTUAL, EXPECTED, "T2 then T1").await;
    assert_eq!(extremes(&d, 1).await, ["(,,t)"], "T2's page probed");
    assert_oracle(&mut d).await;
}

/// F5: the only non-`NULL` value set to `NULL`, then deleted, each its own
/// page in order: the group's extremes go to `NULL` while it still has
/// rows, then the group goes.
#[tokio::test]
async fn f5_the_only_value_set_to_null_then_deleted() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 4), (2, 1, null)",
    )
    .await;
    step(&mut d, "update public.src set v = null where id = 1").await;
    assert_now(&d, ACTUAL, EXPECTED, "set to null").await;
    step(&mut d, "delete from public.src where id = 1").await;
    assert_now(&d, ACTUAL, EXPECTED, "deleted").await;
    step(&mut d, "insert into public.src values (3, 1, 6)").await;
    assert_oracle(&mut d).await;
}

/// F5, the `NULL` group key: the minimum of the `NULL` group deleted, and
/// an insertion below it retracted before it drains. Each probe reads the
/// `NULL` group's rows (`g is null`), not every group's and not none. Fails
/// under `NULLKEY` (the probe matches the group by `=`, finds no row, and
/// leaves the extremes `NULL`) and, in the reversed pair, `NOBASIS`.
#[tokio::test]
async fn f5_the_null_group_probes_its_own_rows() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, null, 1), (2, null, 4), (3, null, 8), (4, 1, 0)",
    )
    .await;
    step(&mut d, "delete from public.src where id = 1").await;
    assert_now(&d, ACTUAL, EXPECTED, "the NULL group's minimum deleted").await;
    write(&d, "insert into public.src values (5, null, 2)").await;
    let insert = d.seal().await;
    write(&d, "delete from public.src where id = 5").await;
    let delete = d.seal().await;
    d.drain(delete, "a").await;
    d.drain(insert, "b").await;
    assert_now(&d, ACTUAL, EXPECTED, "insert-low-delete, reversed").await;
    step(&mut d, "delete from public.src where id = 3").await;
    assert_oracle(&mut d).await;
}

/// F5, two nullable group columns: every pattern of `NULL` keys has its
/// minimum and maximum deleted, so each pattern's probe runs, and each reads
/// only its own group. Fails under `NULLKEY`.
#[tokio::test]
async fn f5_every_pattern_of_null_group_columns_probes_its_own_rows() {
    let mut d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g, h \
         SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n",
        "create table public.src (id integer primary key, g integer, h integer, v integer); \
         create index on public.src (g, h, v)",
        &[("id", int4()), ("g", int4()), ("h", int4()), ("v", int4())],
    )
    .await;
    let actual = "select g, h, lo, hi, n from public.agg order by g, h";
    let expected =
        "select g, h, min(v), max(v), count(*) from public.src group by g, h order by g, h";
    let mut rows = Vec::new();
    for (k, (g, h)) in [("1", "1"), ("1", "null"), ("null", "1"), ("null", "null")]
        .into_iter()
        .enumerate()
    {
        for v in 1..=3 {
            rows.push(format!("({}, {g}, {h}, {v})", k * 10 + v));
        }
    }
    step(
        &mut d,
        &format!("insert into public.src values {}", rows.join(", ")),
    )
    .await;
    for sql in [
        "delete from public.src where id % 10 = 1",
        "delete from public.src where id % 10 = 3",
        "delete from public.src where id % 10 = 2",
    ] {
        step(&mut d, sql).await;
        assert_now(&d, actual, expected, sql).await;
    }
    d.settle().await;
    assert_now(&d, actual, expected, "settled").await;
}

// ----------------------------------------------------------------------- F6

/// F6 (`gc-after-probe`): group 1 is empty when T1 inserts 1 and T2 deletes
/// it and inserts 7. T2's page drains first: its counts net to zero, and
/// its retraction of 1 probes, whose snapshot sees both. The group is then
/// a zero group with no partials' basis, but its extremes' basis saw T1,
/// whose page hasn't drained, so the GC keeps it (Rx7), and T1's insertion
/// is refused for the extremes when it drains. Fails under `GCX`: the GC
/// collects the group, and T1 lands on a fresh row as minimum 1.
#[tokio::test]
async fn f6_a_zero_group_a_probe_stamped_stays_until_the_prefix_passes_it() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (100, 2, 0)").await;
    write(&d, "insert into public.src values (1, 1, 1)").await;
    let t1 = d.seal().await;
    write(
        &d,
        "delete from public.src where id = 1; insert into public.src values (2, 1, 7)",
    )
    .await;
    let t2 = d.seal().await;
    d.drain(t2, "a").await;
    let probed = extremes(&d, 1).await;
    let collected = d.collect_tombstones().await;
    d.drain(t1, "b").await;
    assert_now(&d, ACTUAL, EXPECTED, "T2, the GC, then T1").await;
    assert_eq!(probed, ["(7,7,t)"]);
    assert_eq!(collected, 0, "the probe's stamp kept the zero group");
    assert_oracle(&mut d).await;
}

// ------------------------------------------------------------- self_check

/// Every page of the check of `agg`, `limit` groups at a time, after its
/// convergence wait: the divergences it reports.
async fn walk(d: &Driver, limit: i64) -> Vec<Divergence> {
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = check_page(
            d.pool(),
            "agg",
            SelfCheckScope { after, limit },
            SelfCheckMode::Standard,
            Duration::from_secs(30),
        )
        .await
        .expect("a page of the check");
        match page.outcome {
            SelfCheckOutcome::Converged => {}
            SelfCheckOutcome::Diverged(mut divergences) => found.append(&mut divergences),
            other => panic!("unexpected outcome {other:?}"),
        }
        after = page.next_after;
        if after.is_none() {
            return found;
        }
    }
}

/// Every page of one comparison of `agg`, with no convergence wait and
/// whatever the ring holds: the divergences, and whether any group was
/// uncertain.
async fn compare(d: &Driver, limit: i64) -> (Vec<Divergence>, bool) {
    let mut after = None;
    let (mut found, mut uncertain) = (Vec::new(), false);
    loop {
        let page = compare_page(d.pool(), "agg", SelfCheckScope { after, limit })
            .await
            .expect("a comparison");
        found.extend(page.divergences);
        uncertain |= page.uncertain_all || !page.uncertain_keys.is_empty();
        after = page.next_after;
        if after.is_none() {
            return (found, uncertain);
        }
    }
}

/// [`compare`]'s divergences, without those of the groups it is uncertain
/// of.
async fn compare_certain(d: &Driver, limit: i64) -> Vec<Divergence> {
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = compare_page(d.pool(), "agg", SelfCheckScope { after, limit })
            .await
            .expect("a comparison");
        if !page.uncertain_all {
            found.extend(page.divergences.into_iter().filter(|x| match x {
                Divergence::Cell { key, .. }
                | Divergence::MissingRow { key }
                | Divergence::ExtraRow { key } => !page.uncertain_keys.contains(key),
                _ => true,
            }));
        }
        after = page.next_after;
        if after.is_none() {
            return found;
        }
    }
}

fn cell(key: &str, column: &str, persisted: Option<&str>, recomputed: Option<&str>) -> Divergence {
    Divergence::Cell {
        key: key.to_string(),
        column: column.to_string(),
        persisted: persisted.map(str::to_string),
        recomputed: recomputed.map(str::to_string),
    }
}

/// The hidden table's name, unqualified, as a divergence names its columns.
async fn hidden_name(d: &Driver) -> String {
    hidden(d).await.rsplit('.').next().unwrap().to_string()
}

/// Asserts the walk at every page size reports exactly `want`.
async fn assert_walk(d: &Driver, want: &[Divergence], what: &str) {
    for limit in [1, 2, 100] {
        let mut found = walk(d, limit).await;
        found.sort_by_key(|d| format!("{d:?}"));
        let mut want = want.to_vec();
        want.sort_by_key(|d| format!("{d:?}"));
        assert_eq!(found, want, "{what}, limit {limit}");
    }
}

/// `self_check` compares each group's hidden extremes with a fresh
/// `MIN`/`MAX` of the source: a converged target with ties, a group whose
/// arguments are all `NULL`, the `NULL` group and a group emptied to zero
/// reports nothing at any page size (the zero group collected). A planted extreme is reported on its
/// hidden column whether the visible value was planted with it (the visible
/// row then agrees with the hidden one, and only the source shows it) or
/// alone (the visible row against the hidden extreme), on an ordinary
/// group, on the `NULL` group, on an all-`NULL` group given a value, and on
/// a group whose extreme is set to `NULL`. Fails if the source comparison is
/// skipped, if it reads the other extreme's column, or if a group whose
/// arguments are all `NULL` (or whose key is `NULL`) is left out of it.
#[tokio::test]
async fn self_check_compares_each_extreme_with_the_sources() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values \
         (1, 1, 3), (2, 1, 3), (3, 1, 5), (4, 1, 9), \
         (5, 2, null), (6, 2, null), \
         (7, null, 4), (8, null, 6), \
         (9, 3, 2)",
    )
    .await;
    // A tie at the minimum: one of the two deleted probes and finds the
    // other. Group 3 empties to a zero group.
    step(&mut d, "delete from public.src where id in (1, 9)").await;
    assert_oracle(&mut d).await;
    assert_walk(&d, &[], "converged").await;

    let hidden = hidden(&d).await;
    let name = hidden_name(&d).await;
    let min = format!("{name}.__min_1");
    let max = format!("{name}.__max_1");
    let plant = |sql: String| {
        let d = &d;
        async move { write(d, &sql).await }
    };

    // The hidden and visible minimum planted alike: only the source shows it.
    plant(format!(
        "update {hidden} set __min_1 = 4 where g = 1; update public.agg set lo = 4 where g = 1"
    ))
    .await;
    assert_walk(
        &d,
        &[cell("1", &min, Some("4"), Some("3"))],
        "planted alike",
    )
    .await;
    // Then the visible value alone, back to the source's: the visible row
    // against the hidden extreme.
    plant("update public.agg set lo = 3 where g = 1".to_string()).await;
    assert_walk(
        &d,
        &[
            cell("1", &min, Some("4"), Some("3")),
            cell("1", "lo", Some("3"), Some("4")),
        ],
        "visible restored alone",
    )
    .await;
    plant(format!("update {hidden} set __min_1 = 3 where g = 1")).await;
    assert_walk(&d, &[], "restored").await;

    // The maximum, its own column.
    plant(format!(
        "update {hidden} set __max_1 = 10 where g = 1; update public.agg set hi = 10 where g = 1"
    ))
    .await;
    assert_walk(&d, &[cell("1", &max, Some("10"), Some("9"))], "maximum").await;
    plant(format!(
        "update {hidden} set __max_1 = 9 where g = 1; update public.agg set hi = 9 where g = 1"
    ))
    .await;

    // The visible maximum alone.
    plant("update public.agg set hi = 8 where g = 1".to_string()).await;
    assert_walk(
        &d,
        &[cell("1", "hi", Some("8"), Some("9"))],
        "visible alone",
    )
    .await;
    plant("update public.agg set hi = 9 where g = 1".to_string()).await;

    // The all-`NULL` group given a value, and an ordinary group's extreme
    // set to `NULL`, each alike in the hidden and visible rows.
    plant(format!(
        "update {hidden} set __min_1 = 1 where g = 2; update public.agg set lo = 1 where g = 2; \
         update {hidden} set __max_1 = null where g = 1; update public.agg set hi = null where g = 1"
    ))
    .await;
    assert_walk(
        &d,
        &[
            cell("2", &min, Some("1"), None),
            cell("1", &max, None, Some("9")),
        ],
        "NULL planted and replaced",
    )
    .await;
    plant(format!(
        "update {hidden} set __min_1 = null where g = 2; update public.agg set lo = null where g = 2; \
         update {hidden} set __max_1 = 9 where g = 1; update public.agg set hi = 9 where g = 1"
    ))
    .await;

    // The `NULL` group.
    plant(format!(
        "update {hidden} set __max_1 = 7 where g is null; \
         update public.agg set hi = 7 where g is null"
    ))
    .await;
    let found = walk(&d, 100).await;
    assert!(
        matches!(found.as_slice(), [Divergence::Cell { column, persisted, recomputed, .. }]
            if *column == max && persisted.as_deref() == Some("7")
                && recomputed.as_deref() == Some("6")),
        "the NULL group's maximum: {found:?}"
    );
    plant(format!(
        "update {hidden} set __max_1 = 6 where g is null; \
         update public.agg set hi = 6 where g is null"
    ))
    .await;

    // A hidden row of a group the source doesn't have, with an extreme:
    // reported on its partials and its extreme alike.
    plant(format!(
        "insert into {hidden} (g, __n, __count_1, __min_1, __max_1) values (3, 0, 0, 2, null)"
    ))
    .await;
    assert_walk(
        &d,
        &[cell("3", &min, Some("2"), None)],
        "a zero group's extreme",
    )
    .await;
    plant(format!("delete from {hidden} where g = 3")).await;
    assert_walk(&d, &[], "restored").await;
}

/// A group's extremes are compared with the source under their own basis,
/// `__xbasis`, not the partials'. First the F6 interleaving: T2 deletes T1's
/// row and inserts 7, and T2's page drains first, so its probe saw T1. The
/// group is then a zero group by its partials but keeps the extremes the
/// probe read, and the comparison agrees with it before T1 drains and
/// after, also with changes no basis saw pending. Then a group with rows
/// whose minimum's probe saw an insertion that hasn't drained. Fails if the
/// extremes' source rows subtract the unapplied changes `__xbasis` saw (as
/// the partials' basis would), or keep a value whose insertion is
/// subtracted.
#[tokio::test]
async fn self_check_reads_the_extremes_under_their_own_basis() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (100, 2, 0)").await;
    write(&d, "insert into public.src values (1, 1, 1)").await;
    let t1 = d.seal().await;
    write(
        &d,
        "delete from public.src where id = 1; insert into public.src values (2, 1, 7)",
    )
    .await;
    let t2 = d.seal().await;
    d.drain(t2, "a").await;
    assert_eq!(extremes(&d, 1).await, ["(7,7,t)"]);
    for limit in [1, 2, 100] {
        assert_eq!(compare(&d, limit).await, (vec![], false), "limit {limit}");
    }
    // A plant on the probed extreme is still reported.
    let hidden = hidden(&d).await;
    write(&d, &format!("update {hidden} set __max_1 = 8 where g = 1")).await;
    assert_eq!(
        compare(&d, 100).await,
        (
            vec![cell(
                "1",
                &format!("{}.__max_1", hidden_name(&d).await),
                Some("8"),
                Some("7")
            )],
            false
        )
    );
    write(&d, &format!("update {hidden} set __max_1 = 7 where g = 1")).await;
    // Changes no basis saw, not applied yet: an insertion below the
    // minimum and a delete of the maximum are subtracted from the source.
    write(
        &d,
        "insert into public.src values (3, 1, 0); delete from public.src where id = 2",
    )
    .await;
    assert_eq!(compare(&d, 100).await, (vec![], false));
    d.drain(t1, "b").await;
    assert_eq!(compare(&d, 100).await, (vec![], false));
    assert_oracle(&mut d).await;
    assert_walk(&d, &[], "settled").await;

    // A group with rows: T3 inserts 4, and T4's delete of the minimum 1
    // drains first. Its probe sees T3, which its partials don't count yet.
    step(
        &mut d,
        "insert into public.src values (10, 5, 1), (11, 5, 7)",
    )
    .await;
    write(&d, "insert into public.src values (12, 5, 4)").await;
    let t3 = d.seal().await;
    write(&d, "delete from public.src where id = 10").await;
    let t4 = d.seal().await;
    d.drain(t4, "a").await;
    assert_eq!(extremes(&d, 5).await, ["(4,7,t)"]);
    assert_eq!(compare(&d, 100).await, (vec![], false));
    d.drain(t3, "b").await;
    assert_eq!(compare(&d, 100).await, (vec![], false));
    assert_oracle(&mut d).await;
}

/// The groups only the extremes' side has. Group 1 is F6's zero group, whose
/// extremes the source's partials don't show. Its hidden row lost is
/// reported on its extremes alone (a later insertion would set the extreme
/// from `NULL`). Group 2's hidden row lost is reported on group 2 only, at
/// every page size: at limit 2 the extremes' side (groups 1 and 2) ends the
/// page before the hidden side (1 and 3) and the source's (2 and 3), so
/// group 3 isn't compared against an extremes' side that stopped short of
/// it. Fails if side 7 doesn't bound the page, or if a group only side 7
/// has isn't compared.
#[tokio::test]
async fn self_check_compares_the_groups_only_the_extremes_have() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (100, 2, 0), (300, 3, 5)",
    )
    .await;
    write(&d, "insert into public.src values (1, 1, 1)").await;
    let t1 = d.seal().await;
    write(
        &d,
        "delete from public.src where id = 1; insert into public.src values (2, 1, 7)",
    )
    .await;
    let t2 = d.seal().await;
    d.drain(t2, "a").await;
    assert_eq!(extremes(&d, 1).await, ["(7,7,t)"]);
    for limit in [1, 2, 100] {
        assert_eq!(compare(&d, limit).await, (vec![], false), "limit {limit}");
    }
    let hidden = hidden(&d).await;
    let name = hidden_name(&d).await;
    let save = |g: i32| {
        format!(
            "create table public.saved as select * from {hidden} where g = {g}; \
             delete from {hidden} where g = {g}"
        )
    };
    // The hidden table's stored columns: a generated one takes no value.
    let stored = scalar(
        &d,
        &format!(
            "select string_agg(attname, '|' order by attnum) from pg_attribute \
             where attrelid = '{hidden}'::regclass and attnum > 0 and not attisdropped \
               and attgenerated = ''"
        ),
    )
    .await
    .replace('|', ", ");
    let restore = format!(
        "insert into {hidden} ({stored}) select {stored} from public.saved; \
         drop table public.saved"
    );

    write(&d, &save(1)).await;
    for limit in [1, 2, 100] {
        assert_eq!(
            compare(&d, limit).await,
            (
                vec![
                    cell("1", &format!("{name}.__min_1"), None, Some("7")),
                    cell("1", &format!("{name}.__max_1"), None, Some("7")),
                ],
                false
            ),
            "limit {limit}"
        );
    }
    write(&d, &restore).await;

    write(&d, &save(2)).await;
    for limit in [1, 2, 100] {
        let (found, _) = compare(&d, limit).await;
        assert!(
            !found.is_empty()
                && found.iter().all(|x| matches!(x,
                    Divergence::Cell { key, .. } | Divergence::ExtraRow { key } if key == "2")),
            "limit {limit}: {found:?}"
        );
    }
    write(&d, &restore).await;

    d.drain(t1, "b").await;
    assert_oracle(&mut d).await;
    assert_walk(&d, &[], "settled").await;
}

/// Extremes of two arguments, each `NULL` on rows where the other isn't:
/// side 7 sums weights per (group, both arguments) and takes each extreme
/// over the rows its own argument isn't `NULL` on, through pending changes
/// and after they drain. Fails if a row with either argument `NULL` is left
/// out of both extremes.
#[tokio::test]
async fn self_check_takes_each_extreme_over_its_own_arguments() {
    let mut d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g SELECT MIN(v) AS lo, MAX(w) AS hi",
        "create table public.src (id integer primary key, g integer, v integer, w integer); \
         create index on public.src (g, v); create index on public.src (g, w)",
        &[("id", int4()), ("g", int4()), ("v", int4()), ("w", int4())],
    )
    .await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 5, null), (2, 1, null, 9), (3, 1, 6, 2), \
         (4, 2, null, 3), (5, 3, 4, null)",
    )
    .await;
    assert_walk(&d, &[], "settled").await;
    write(
        &d,
        "insert into public.src values (6, 1, 1, null); update public.src set w = 10 where id = 4",
    )
    .await;
    let pending = d.seal().await;
    for limit in [1, 2, 100] {
        assert_eq!(compare(&d, limit).await, (vec![], false), "limit {limit}");
    }
    d.drain(pending, "late").await;
    assert_walk(&d, &[], "drained").await;
    assert_now(
        &d,
        "select g, lo, hi from public.agg order by g",
        "select g, min(v), max(w) from public.src group by g order by g",
        "drained",
    )
    .await;
}

/// One definition `MIN(v) AS lo, MAX(v) AS hi` over `v` of type `ty`, seeded
/// with `rows` (`(id, g, v)` literals) and settled.
async fn start_typed(ty: &str, value_type: ValueType, rows: &str) -> Driver {
    let mut d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g SELECT MIN(v) AS lo, MAX(v) AS hi",
        &format!(
            "create table public.src (id integer primary key, g integer, v {ty}); \
             create index on public.src (g, v)"
        ),
        &[("id", int4()), ("g", int4()), ("v", value_type)],
    )
    .await;
    step(&mut d, &format!("insert into public.src values {rows}")).await;
    d.settle().await;
    assert_walk(&d, &[], "settled").await;
    d
}

/// For each `(hidden and visible extreme, its column, source's extreme)`,
/// plants the value in both the hidden table and the target of group 1 and
/// asserts the check reports the hidden column exactly when `diverged`.
async fn assert_planted(d: &Driver, plants: &[(&str, &str, &str, bool)]) {
    let hidden = hidden(d).await;
    let name = hidden_name(d).await;
    for (value, stem, source, diverged) in plants {
        let field = if *stem == "min" { "lo" } else { "hi" };
        write(
            d,
            &format!(
                "update {hidden} set __{stem}_1 = '{value}' where g = 1; \
                 update public.agg set {field} = '{value}' where g = 1"
            ),
        )
        .await;
        let want = if *diverged {
            vec![cell(
                "1",
                &format!("{name}.__{stem}_1"),
                Some(value),
                Some(source),
            )]
        } else {
            vec![]
        };
        assert_walk(d, &want, &format!("{stem} planted as {value}")).await;
        write(
            d,
            &format!(
                "update {hidden} set __{stem}_1 = '{source}' where g = 1; \
                 update public.agg set {field} = '{source}' where g = 1"
            ),
        )
        .await;
    }
}

/// Equal-but-distinguishable extremes are one equal value (#1117 rule 3,
/// ADR-0020): an extreme that renders `1.000` against the source's `1.0`
/// is no divergence, and one that differs by value is. A group whose
/// arguments tie by value at different scales (`1.0`, `1.00`) converges
/// whichever of them each side shows. Fails if extremes are compared as
/// text.
#[tokio::test]
async fn numeric_extremes_compare_by_the_types_equality() {
    let d = start_typed(
        "numeric",
        ValueType::Numeric,
        "(1, 1, 1.0), (2, 1, 1.00), (3, 1, 2.50), (4, 1, 2.5), (5, 2, 'NaN'), (6, 2, 0)",
    )
    .await;
    assert_planted(
        &d,
        &[
            ("1.000", "min", "1.0", false),
            ("1.001", "min", "1.0", true),
            ("2.500000", "max", "2.50", false),
            ("2.4", "max", "2.50", true),
        ],
    )
    .await;
}

/// `float8`: `-0` and `0` are equal, `NaN` equals `NaN` (Postgres's float
/// `=`), and the infinities are values like any other. Fails if extremes
/// are compared as text.
#[tokio::test]
async fn float_extremes_compare_by_the_types_equality() {
    let d = start_typed(
        "double precision",
        ValueType::Float(FloatWidth::Float8),
        "(1, 1, 0), (2, 1, 'NaN'), (3, 1, 3), (4, 2, '-Infinity'), (5, 2, 'Infinity')",
    )
    .await;
    assert_planted(
        &d,
        &[
            ("-0", "min", "0", false),
            ("1e-300", "min", "0", true),
            ("NaN", "max", "NaN", false),
            ("Infinity", "max", "NaN", true),
            ("-Infinity", "min", "0", true),
        ],
    )
    .await;
}

/// `interval`: `'1 day'` and `'24 hours'` are equal and render apart.
/// Fails if extremes are compared as text.
#[tokio::test]
async fn interval_extremes_compare_by_the_types_equality() {
    let d = start_typed(
        "interval",
        ValueType::Other(PgType::Interval),
        "(1, 1, '1 day'), (2, 1, '3 days'), (3, 2, '1 hour')",
    )
    .await;
    assert_planted(
        &d,
        &[
            ("24:00:00", "min", "1 day", false),
            ("25:00:00", "min", "1 day", true),
            ("72:00:00", "max", "3 days", false),
            ("1 mon", "max", "3 days", true),
        ],
    )
    .await;
}

/// `timestamptz`, with its infinities: one text per value (the check's own
/// session renders both sides). A converged target reports nothing, and a
/// value off by a microsecond or an infinity is reported.
#[tokio::test]
async fn timestamptz_extremes_compare_exactly() {
    let d = start_typed(
        "timestamptz",
        ValueType::Other(PgType::TimestampTz),
        "(1, 1, '-infinity'), (2, 1, '2026-01-01 00:00:00.000001+00'), (3, 1, 'infinity'), \
         (4, 2, '2026-06-01 12:00:00+00')",
    )
    .await;
    assert_planted(
        &d,
        &[
            ("2026-01-01 00:00:00+00", "min", "-infinity", true),
            ("infinity", "min", "-infinity", true),
            ("2026-01-01 00:00:00.000001+00", "max", "infinity", true),
        ],
    )
    .await;
}

/// `bigint` at its limits: a value off by one is reported.
#[tokio::test]
async fn bigint_extremes_compare_exactly() {
    let d = start_typed(
        "bigint",
        ValueType::Integer(IntWidth::Int8),
        "(1, 1, -9223372036854775808), (2, 1, 0), (3, 1, 9223372036854775807), (4, 2, 7)",
    )
    .await;
    assert_planted(
        &d,
        &[
            ("-9223372036854775807", "min", "-9223372036854775808", true),
            ("9223372036854775806", "max", "9223372036854775807", true),
        ],
    )
    .await;
}

// ------------------------------------------------------------ source lock

/// The probe reads the source, so it takes the source's `ACCESS SHARE`
/// lock while the page holds its groups (Rx5, I7). Behind an application's
/// `ACCESS EXCLUSIVE` lock it waits under the page's `lock_timeout`: here
/// 100 ms, so it times out, rolls back and is retried by the drain, and the
/// page applies once the application's transaction ends.
#[tokio::test]
async fn a_probe_behind_a_source_lock_retries_the_page() {
    let mut d = Driver::start_with_database_defaults(
        &[("lock_timeout", "100ms")],
        SOURCE_DDL,
        &[("id", int4()), ("g", int4()), ("v", int4())],
        &[AGG],
        &[SRC],
    )
    .await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 1), (2, 1, 5), (3, 2, 4)",
    )
    .await;
    write(&d, "delete from public.src where id = 1").await;
    let batch = d.seal().await;
    let holder = d.user().await;
    let pid: i32 = holder
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    holder
        .batch_execute("begin; lock table public.src in access exclusive mode")
        .await
        .expect("the application's lock");
    let page = d.drain_frozen(batch, "a", &[]).await;
    // The page's transaction waiting behind the application's lock: it
    // times out and a later transaction waits in its place, while the
    // application still holds the lock. A probe that waited without a
    // `lock_timeout` would stay in its first transaction.
    let waiting = "select pid, xact_start::text from pg_stat_activity \
                   where $1 = any(pg_blocking_pids(pid))";
    d.wait_blocked_behind(pid).await;
    let watcher = d.user().await;
    let waiters = || async {
        watcher
            .query(waiting, &[&pid])
            .await
            .expect("the waiting page")
            .into_iter()
            .map(|row| (row.get::<_, i32>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>()
    };
    let first = waiters().await;
    assert_eq!(first.len(), 1, "one page waits: {first:?}");
    // It waits for the source while it holds its groups: the probe, after
    // the lock statement.
    let held: bool = watcher
        .query_one(
            "select exists (select 1 from pg_locks where pid = $1 and not granted \
                              and relation = 'public.src'::regclass) \
                and exists (select 1 from pg_locks where pid = $1 and granted \
                              and relation = $2::text::regclass)",
            &[&first[0].0, &hidden(&d).await],
        )
        .await
        .expect("the waiting page's locks")
        .get(0);
    assert!(held, "the page waits for the source holding its groups");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while waiters().await.iter().all(|w| *w == first[0]) {
        assert!(
            std::time::Instant::now() < deadline,
            "the page's first transaction ({first:?}) never timed out and retried"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    holder.batch_execute("commit").await.expect("commit");
    page.finish().await;
    assert_eq!(extremes(&d, 1).await, ["(5,5,t)"]);
    assert_oracle(&mut d).await;
}
