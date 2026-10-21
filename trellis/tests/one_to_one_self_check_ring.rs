//! `self_check` of a 1-1 target compares it against its source minus the ring
//! rows the target hasn't applied yet (#1176, ADR-0013 "Quiescence"): a key
//! with an unapplied change is expected as that change's old image (no row
//! for an insert), any other key as its live row.
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`
//! and never polls for convergence (#297): a page is compared while a sealed
//! segment nobody drained, or the active segment, still holds changes.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::self_check::{Divergence, SelfCheckScope, compare_page};

const SRC: &str = "public.src";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn start() -> Driver {
    Driver::start(
        "create table public.src (id integer primary key, v integer, w integer)",
        &[("id", int4()), ("v", int4()), ("w", int4())],
        &["TRANSFORM t FROM public.src SELECT v + w AS total, w AS w"],
        &[SRC],
    )
    .await
}

async fn step(d: &mut Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

async fn exec(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Every page of one comparison of `t`, `limit` rows at a time, with no wait
/// and no re-check: the divergences, the keys it couldn't vouch for exactly,
/// and whether it couldn't vouch for any.
async fn compare(d: &Driver, limit: i64) -> (Vec<Divergence>, Vec<String>, bool) {
    let mut after = None;
    let (mut found, mut uncertain, mut all) = (Vec::new(), Vec::new(), false);
    loop {
        let page = compare_page(d.pool(), "t", SelfCheckScope { after, limit })
            .await
            .expect("a comparison");
        found.extend(page.divergences);
        uncertain.extend(page.uncertain_keys);
        all |= page.uncertain_all;
        after = page.next_after;
        if after.is_none() {
            uncertain.sort();
            uncertain.dedup();
            return (found, uncertain, all);
        }
    }
}

fn cell(key: &str, column: &str, persisted: &str, recomputed: &str) -> Divergence {
    Divergence::Cell {
        key: key.to_string(),
        column: column.to_string(),
        persisted: Some(persisted.to_string()),
        recomputed: Some(recomputed.to_string()),
    }
}

/// Changes of every kind the drain hasn't applied, several to one key, some
/// in a sealed segment nobody drained and some in the active one, against a
/// correct target: one pass finds nothing at every page size.
#[tokio::test]
async fn pending_changes_to_a_correct_target_give_no_divergence_on_one_pass() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 10, 1), (2, 20, 2), (3, 30, 3), (4, 40, 4)",
    )
    .await;
    exec(
        &d,
        "update public.src set v = 11 where id = 1; update public.src set v = 12 where id = 1; \
         delete from public.src where id = 2; insert into public.src values (5, 50, 5)",
    )
    .await;
    let sealed = d.seal().await;
    exec(
        &d,
        "update public.src set w = 9 where id = 3; update public.src set w = 10 where id = 3; \
         delete from public.src where id = 4; insert into public.src values (4, 41, 4); \
         update public.src set v = 51 where id = 5",
    )
    .await;
    for limit in [1, 2, 100] {
        assert_eq!(
            compare(&d, limit).await,
            (vec![], vec![], false),
            "limit {limit}"
        );
    }
    d.drain(sealed, "late").await;
    assert_eq!(compare(&d, 2).await, (vec![], vec![], false));
    let rest = d.seal().await;
    d.drain(rest, "late").await;
    assert_eq!(compare(&d, 2).await, (vec![], vec![], false));
}

/// A planted error on a key with pending changes is reported with its exact
/// values, in one pass, and so is one on a quiet key.
#[tokio::test]
async fn a_planted_error_is_caught_with_pending_changes_present() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 10, 1), (2, 20, 2), (3, 30, 3)",
    )
    .await;
    exec(
        &d,
        "update public.t set total = total + 1000 where id in (1, 3)",
    )
    .await;
    exec(
        &d,
        "update public.src set v = 11 where id = 1; update public.src set v = 12 where id = 1",
    )
    .await;
    let (found, uncertain, all) = compare(&d, 10).await;
    // The hot key's target row is the old image's plus the plant; the quiet
    // key's is the live row's plus the plant.
    assert_eq!(
        found,
        [
            cell("1", "total", "1011", "11"),
            cell("3", "total", "1033", "33")
        ]
    );
    assert_eq!((uncertain, all), (vec![], false));
}

/// A pending insert expects no row, so a target row for it is an extra, and
/// a pending delete expects the row, so its absence is a missing row.
#[tokio::test]
async fn a_pending_insert_expects_no_row_and_a_pending_delete_expects_one() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 10, 1), (2, 20, 2)",
    )
    .await;
    exec(
        &d,
        "insert into public.src values (3, 30, 3); delete from public.src where id = 2",
    )
    .await;
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    exec(&d, "delete from public.t where id = 2").await;
    exec(&d, "insert into public.t (id, total, w) values (3, 33, 3)").await;
    let (found, _, _) = compare(&d, 10).await;
    assert_eq!(
        found,
        [
            Divergence::MissingRow { key: "2".into() },
            Divergence::ExtraRow { key: "3".into() },
        ]
    );
}

/// A change with no image (a `Recompute`) can't be subtracted: its key is
/// the only one the comparison can't vouch for, and falls back to the live
/// row.
#[tokio::test]
async fn an_image_less_pending_change_leaves_only_its_key_uncertain() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 10, 1), (2, 20, 2)",
    )
    .await;
    exec(&d, "update public.src set v = 11 where id = 2").await;
    d.stage_recomputes(SRC, &["1"]).await;
    let (found, uncertain, all) = compare(&d, 10).await;
    assert_eq!(found, []);
    assert_eq!((uncertain, all), (vec!["1".to_string()], false));
}

/// A source `TRUNCATE` the drain hasn't applied can't be subtracted.
#[tokio::test]
async fn a_pending_truncate_leaves_every_key_uncertain() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 10, 1)").await;
    exec(&d, "truncate public.src").await;
    let (_, _, all) = compare(&d, 10).await;
    assert!(all);
}
