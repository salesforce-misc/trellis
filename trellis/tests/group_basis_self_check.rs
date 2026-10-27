//! `self_check` of an aggregate target on the group basis (ADR-0013
//! "Scope"). It detects and never repairs: a planted divergence is reported,
//! and gone once the planted damage is undone (the repair for real damage
//! is `request_backfill` of the source, which rebuilds the groups in place:
//! `group_basis_rebuild.rs`; the last test here runs the whole loop, check,
//! `request_backfill`, check).
//!
//! Every test turns the group-basis dev knob on before its driver starts,
//! drives the pipeline by hand through `support/drain_driver.rs` and never
//! polls for convergence: a check either runs on a drained pipeline or
//! meets a sealed page nobody drained.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};
use trellis::{Trellis, TrellisOptions};

const SRC: &str = "public.src";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn start_with(definition: &str, source_ddl: &str, columns: &[(&str, ValueType)]) -> Driver {
    Driver::start(source_ddl, columns, &[definition], &[SRC]).await
}

async fn start() -> Driver {
    start_with(
        AGG,
        "create table public.src (id integer primary key, g integer, v integer); \
         create index on public.src (g)",
        &[("id", int4()), ("g", int4()), ("v", int4())],
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

async fn hidden(d: &Driver) -> String {
    let id = d
        .rows("select id from transform_definitions where target_table = 'public.agg'")
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    format!("{DEFAULT_SCHEMA}.agg_{id}")
}

/// Every page of the check of `agg`, `limit` groups at a time: the
/// divergences, the groups compared and the number of pages.
async fn walk(d: &Driver, limit: i64) -> (Vec<Divergence>, i64, usize) {
    let mut after = None;
    let (mut found, mut compared, mut pages) = (Vec::new(), 0, 0);
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
        pages += 1;
        compared += page.rows_compared;
        match page.outcome {
            SelfCheckOutcome::Converged => {}
            SelfCheckOutcome::Diverged(mut divergences) => found.append(&mut divergences),
            other => panic!("unexpected outcome {other:?}"),
        }
        after = page.next_after;
        if after.is_none() {
            return (found, compared, pages);
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

/// A converged target, whatever its groups went through, reports nothing,
/// page by page: groups that moved, a `NULL` group, `NULL` arguments, and a
/// group that emptied (its hidden row stays, at zero).
#[tokio::test]
async fn a_converged_group_target_reports_no_divergence_page_by_page() {
    let mut d = start().await;
    for sql in [
        "insert into public.src values (1, 1, 10), (2, 1, 5), (3, 2, null), (4, null, 7), \
                                       (5, 3, 1), (6, 4, 2)",
        "update public.src set g = 2 where id = 2",
        "delete from public.src where id in (5, 6)",
        "update public.src set v = null where id = 1",
    ] {
        step(&mut d, sql).await;
    }
    for limit in [1, 2, 100] {
        let (found, compared, pages) = walk(&d, limit).await;
        assert_eq!(found, [], "limit {limit}");
        // Every group is reached, on whichever side the page's end falls.
        assert_eq!(compared, 5, "limit {limit}");
        if limit == 1 {
            // Three live groups (one `NULL`) and two emptied ones.
            assert!(pages >= 4, "paged by groups: {pages}");
        }
    }
    let (_, compared, _) = walk(&d, 100).await;
    assert_eq!(compared, 5, "the live groups and the emptied ones");
}

/// Composed fields, an average and a `numeric` sum compare byte for byte.
#[tokio::test]
async fn composed_fields_and_numerics_converge_without_divergence() {
    let mut d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g \
        WITHOUT SOURCE INDEX SELECT SUM(m) AS total, AVG(m) AS mean, SUM(a) + COUNT(m) AS mixed, COUNT(*) AS n",
        "create table public.src (id integer primary key, g integer, a integer, m numeric(10, 2)); \
         create index on public.src (g)",
        &[
            ("id", int4()),
            ("g", int4()),
            ("a", int4()),
            ("m", ValueType::Numeric),
        ],
    )
    .await;
    for sql in [
        "insert into public.src values (1, 1, 1, 1.5), (2, 1, 2, 0.25), (3, 1, 3, null), \
                                       (4, 2, null, 2)",
        "delete from public.src where id = 2",
        "update public.src set m = 3, g = 2 where id = 1",
    ] {
        step(&mut d, sql).await;
        assert_eq!(walk(&d, 2).await.0, [], "after {sql}");
    }
}

/// A sealed page nobody has drained yet is "not caught up", never a
/// divergence: the source is ahead of both the partials and the target.
#[tokio::test]
async fn a_page_still_to_apply_is_not_caught_up_rather_than_diverged() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    exec(&d, "insert into public.src values (2, 1, 5), (3, 2, 1)").await;
    let batch = d.seal().await;
    let page = check_page(
        d.pool(),
        "agg",
        SelfCheckScope {
            after: None,
            limit: 10,
        },
        SelfCheckMode::Standard,
        Duration::from_millis(300),
    )
    .await
    .expect("a page of the check");
    assert_eq!(page.outcome, SelfCheckOutcome::NotCaughtUp);
    d.drain(batch, "late").await;
    assert_eq!(walk(&d, 10).await.0, []);
}

/// A hand-edited partial is reported on its group, naming the hidden table's
/// column, and so is the visible row it no longer matches; undoing the edit
/// clears both.
#[tokio::test]
async fn a_hand_edited_partial_is_reported_until_it_is_restored() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 1, 5), (3, 2, 7)",
    )
    .await;
    let hidden = hidden(&d).await;
    let name = hidden.rsplit('.').next().unwrap().to_string();

    exec(
        &d,
        &format!("update {hidden} set __sum_1 = __sum_1 + 1 where g = 1"),
    )
    .await;
    let (found, _, _) = walk(&d, 10).await;
    // The visible row, which the partials now disagree with, is reported too.
    assert_eq!(
        found,
        [
            cell("1", &format!("{name}.__sum_1"), Some("16"), Some("15")),
            cell("1", "total", Some("15"), Some("16")),
        ]
    );

    exec(
        &d,
        &format!("update {hidden} set __sum_1 = __sum_1 - 1 where g = 1"),
    )
    .await;
    assert_eq!(walk(&d, 10).await.0, []);

    // The visible row against the partials, in the same walk.
    exec(&d, "update public.agg set total = 99 where g = 2").await;
    assert_eq!(
        walk(&d, 1).await.0,
        [cell("2", "total", Some("99"), Some("7"))]
    );
    exec(&d, "update public.agg set total = 7 where g = 2").await;
    assert_eq!(walk(&d, 1).await.0, []);
}

/// A visible row missing, a visible row of no group, a partial of a group
/// the source no longer has, and a group the source no longer has kept
/// consistently in both the partials and the target, each on its own key.
#[tokio::test]
async fn missing_extra_and_stale_groups_are_reported() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 5), (3, null, 1)",
    )
    .await;
    let hidden = hidden(&d).await;
    let name = hidden.rsplit('.').next().unwrap().to_string();

    exec(&d, "delete from public.agg where g = 1").await;
    exec(&d, "insert into public.agg (g, total, n) values (9, 1, 1)").await;
    let (found, _, _) = walk(&d, 2).await;
    assert!(
        found.contains(&Divergence::MissingRow { key: "1".into() }),
        "{found:?}"
    );
    assert!(
        found.contains(&Divergence::ExtraRow { key: "9".into() }),
        "{found:?}"
    );
    assert_eq!(found.len(), 2, "{found:?}");

    // A group the source never had, in the hidden table with a count.
    exec(&d, "delete from public.agg where g = 9").await;
    exec(
        &d,
        &format!("insert into {hidden} (g, __n, __count_1, __sum_1) values (8, 1, 1, 3)"),
    )
    .await;
    let (found, _, _) = walk(&d, 3).await;
    let key = "8";
    for column in ["__n", "__count_1", "__sum_1"] {
        assert!(
            found
                .iter()
                .any(|d| matches!(d, Divergence::Cell { key: k, column: c, .. }
                if k == key && *c == format!("{name}.{column}"))),
            "{column}: {found:?}"
        );
    }
    // The partials imply a visible row the target lacks.
    assert!(
        found.contains(&Divergence::MissingRow { key: key.into() }),
        "{found:?}"
    );

    // The same group with its visible row: the partials and the target
    // agree with each other, so only the source says the row is extra.
    exec(&d, "insert into public.agg (g, total, n) values (8, 3, 1)").await;
    let (found, _, _) = walk(&d, 3).await;
    assert!(
        found.contains(&Divergence::ExtraRow { key: key.into() }),
        "{found:?}"
    );
    assert!(
        !found.contains(&Divergence::MissingRow { key: key.into() }),
        "{found:?}"
    );
    assert_eq!(
        found.len(),
        5,
        "group 1's missing row, and group 8's three partials and row: {found:?}"
    );
}

/// Capture disabled around a write leaves the partials and the target stale
/// by that write; the check reports it once capture is back, and the
/// capture audit alone reports it while it is off.
#[tokio::test]
async fn a_write_made_while_capture_was_off_is_reported() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 5)",
    )
    .await;
    exec(
        &d,
        "alter table public.src disable trigger trellis_capture_insert",
    )
    .await;
    exec(&d, "insert into public.src values (3, 1, 100), (4, 3, 1)").await;
    let off = check_page(
        d.pool(),
        "agg",
        SelfCheckScope {
            after: None,
            limit: 10,
        },
        SelfCheckMode::Standard,
        Duration::from_secs(30),
    )
    .await
    .expect("a page of the check");
    match off.outcome {
        SelfCheckOutcome::Diverged(found) => assert!(
            !found.is_empty() && found.iter().all(|d| matches!(d, Divergence::Capture(_))),
            "{found:?}"
        ),
        other => panic!("expected the capture fault, got {other:?}"),
    }
    exec(
        &d,
        "alter table public.src enable always trigger trellis_capture_insert",
    )
    .await;
    d.settle().await;

    let (found, _, _) = walk(&d, 10).await;
    let hidden = hidden(&d).await;
    let name = hidden.rsplit('.').next().unwrap().to_string();
    assert!(
        found.contains(&cell(
            "1",
            &format!("{name}.__sum_1"),
            Some("10"),
            Some("110")
        )),
        "{found:?}"
    );
    assert!(
        found.contains(&cell("3", &format!("{name}.__n"), Some("0"), Some("1"))),
        "{found:?}"
    );
    // A group neither the partials nor the target have.
    assert!(
        found.contains(&Divergence::MissingRow { key: "3".into() }),
        "{found:?}"
    );
}

/// A two-column group key with a `NULL` in either part, expression
/// arguments and a field over a group key: converged, it reports nothing at
/// any page size; a hand-edited visible row on a key with a `NULL` part is
/// reported on that one cell, so both sides encode the key alike.
#[tokio::test]
async fn composite_keys_with_null_parts_and_expression_arguments() {
    let mut d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g, h \
        WITHOUT SOURCE INDEX SELECT SUM(a + b) AS ab, COUNT(a + b) AS nab, COALESCE(SUM(b), 0) AS sb, \
                SUM(a) > COUNT(b) AS apart, h + COUNT(*) AS hp",
        "create table public.src (id integer primary key, g integer, h integer, \
                                  a integer, b integer); \
         create index on public.src (g, h)",
        &[
            ("id", int4()),
            ("g", int4()),
            ("h", int4()),
            ("a", int4()),
            ("b", int4()),
        ],
    )
    .await;
    for sql in [
        "insert into public.src values (1, 1, 1, 1, 2), (2, 1, null, 3, null), \
                                       (3, null, 1, null, 4), (4, null, null, 5, 6), \
                                       (5, 1, 1, 7, 8), (6, 2, 2, 1, 1)",
        "update public.src set h = null where id = 5",
        "delete from public.src where id = 6",
        "update public.src set b = null where id = 1",
    ] {
        step(&mut d, sql).await;
    }
    for limit in [1, 2, 100] {
        let (found, compared, _) = walk(&d, limit).await;
        assert_eq!(found, [], "limit {limit}");
        assert_eq!(compared, 5, "four live groups and an emptied one");
    }

    exec(
        &d,
        "update public.agg set sb = 99 where g = 1 and h is null",
    )
    .await;
    let (found, _, _) = walk(&d, 2).await;
    assert!(
        matches!(found.as_slice(), [Divergence::Cell { column, persisted, .. }]
            if column == "sb" && persisted.as_deref() == Some("99")),
        "{found:?}"
    );
}

/// Whichever side fills a page first bounds it: a key only the target has,
/// or only the source has, sorting between the others' keys (`"15"` between
/// `"1"` and `"2"`) is reported alike at every page size, and no key past a
/// side's cut is read as missing on it.
#[tokio::test]
async fn a_key_on_one_side_only_is_reported_alike_at_every_page_size() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 5), (3, 3, 1), (4, 4, 2)",
    )
    .await;
    let same_at_every_limit = |found: Vec<Vec<Divergence>>| {
        let first = &found[0];
        for (i, other) in found.iter().enumerate() {
            assert_eq!(
                other.len(),
                first.len(),
                "limit #{i}: {other:?} vs {first:?}"
            );
            assert!(
                other.iter().all(|d| first.contains(d)),
                "limit #{i}: {other:?}"
            );
        }
        first.clone()
    };

    // Only the target has group 15.
    exec(&d, "insert into public.agg (g, total, n) values (15, 1, 1)").await;
    let mut found = Vec::new();
    for limit in [1, 2, 3, 100] {
        found.push(walk(&d, limit).await.0);
    }
    assert_eq!(
        same_at_every_limit(found),
        [Divergence::ExtraRow { key: "15".into() }]
    );
    exec(&d, "delete from public.agg where g = 15").await;

    // Only the source has group 15.
    exec(
        &d,
        "alter table public.src disable trigger trellis_capture_insert",
    )
    .await;
    exec(&d, "insert into public.src values (5, 15, 7)").await;
    exec(
        &d,
        "alter table public.src enable always trigger trellis_capture_insert",
    )
    .await;
    d.settle().await;
    let mut found = Vec::new();
    for limit in [1, 2, 3, 100] {
        found.push(walk(&d, limit).await.0);
    }
    let found = same_at_every_limit(found);
    assert!(
        found.contains(&Divergence::MissingRow { key: "15".into() }),
        "{found:?}"
    );
    assert_eq!(found.len(), 4, "three partials and the row: {found:?}");
}

// ------------------------------------- the ring rows not applied yet

/// Every page of one comparison of `agg`, `limit` groups at a time, with no
/// convergence wait and no re-check, whatever the ring holds: the
/// divergences, the groups it couldn't vouch for exactly, and whether it
/// couldn't vouch for any.
async fn compare(d: &Driver, limit: i64) -> (Vec<Divergence>, Vec<String>, bool) {
    let mut after = None;
    let (mut found, mut uncertain, mut all) = (Vec::new(), Vec::new(), false);
    loop {
        let page = compare_page(d.pool(), "agg", SelfCheckScope { after, limit })
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

/// Changes of every kind the drain hasn't applied, some in a sealed segment
/// nobody drained and some still in the active one, against a correct
/// target: the comparison reads the source minus them, so it finds nothing,
/// at every page size, in one pass. Applying them changes nothing either.
#[tokio::test]
async fn ring_rows_not_applied_yet_are_subtracted_from_the_source() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 1, 5), (3, 2, 7), (4, null, 1),                                        (5, 3, 2)",
    )
    .await;
    exec(
        &d,
        "insert into public.src values (6, 1, 1), (7, 4, 3);          update public.src set g = 2 where id = 2;          update public.src set v = null where id = 1",
    )
    .await;
    let sealed = d.seal().await;
    exec(
        &d,
        "delete from public.src where id = 5;          update public.src set g = null, v = 9 where id = 3;          insert into public.src values (8, 5, 4);          update public.src set v = v + 1 where id = 8",
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
    assert_eq!(walk(&d, 2).await.0, []);
}

/// A planted partial error on a group the ring holds changes for is
/// reported with its exact delta, in one pass, and so is the visible row the
/// partials no longer imply: the changes not applied yet are subtracted, so
/// what remains is the plant.
#[tokio::test]
async fn a_planted_error_on_a_group_with_pending_changes_is_reported_exactly() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 1, 5), (3, 2, 7)",
    )
    .await;
    let hidden = hidden(&d).await;
    let name = hidden.rsplit('.').next().unwrap().to_string();
    exec(
        &d,
        &format!("update {hidden} set __sum_1 = __sum_1 + 1000000 where g = 1"),
    )
    .await;
    exec(
        &d,
        "insert into public.src values (4, 1, 100); update public.src set v = 6 where id = 2",
    )
    .await;
    let (found, uncertain, all) = compare(&d, 10).await;
    assert_eq!(
        found,
        [
            cell("1", &format!("{name}.__sum_1"), Some("1000015"), Some("15")),
            cell("1", "total", Some("15"), Some("1000015")),
        ]
    );
    assert_eq!((uncertain, all), (vec![], false));
}

/// A change with no image (a `Recompute`) can't be subtracted: the groups it
/// may touch, its key's current row's and its prior image's, are the only
/// ones the comparison can't vouch for, and only a divergence on one of them
/// is re-checked.
#[tokio::test]
async fn an_image_less_pending_change_leaves_only_its_groups_uncertain() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 5), (3, 3, 7)",
    )
    .await;
    d.stage_recomputes(SRC, &["1"]).await;
    assert_eq!(
        compare(&d, 10).await,
        (vec![], vec!["1".to_string()], false)
    );
    d.stage_recomputes_with_prior(
        SRC,
        &[("2", Some(r#"{"id": 2, "g": 4, "v": 5}"#.to_string()))],
    )
    .await;
    let (found, uncertain, all) = compare(&d, 1).await;
    assert_eq!(found, []);
    assert_eq!(uncertain, ["1", "2", "4"]);
    assert!(!all);
}

/// A key the definition holds: its changes the drain parked (applied, as the
/// drain counts it, but left out of the target) and those it hasn't reached
/// yet are both subtracted, so the comparison finds nothing while the key is
/// held; its release applies them.
#[tokio::test]
async fn a_held_keys_parked_and_pending_changes_are_subtracted() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 20)",
    )
    .await;
    d.ctl
        .execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, $1, '1', 'test' from transform_definitions \
             where target_table = 'public.agg'",
            &[&SRC],
        )
        .await
        .expect("hold the key");
    step(&mut d, "update public.src set g = 2, v = 11 where id = 1").await;
    assert_eq!(
        d.rows("select count(*) from poison_held").await,
        ["(1)"],
        "the change was parked"
    );
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    exec(&d, "update public.src set g = 3 where id = 1").await;
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    let batch = d.seal().await;
    d.drain(batch, "parks").await;
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    trellis::staging::release_key(d.pool(), "agg", SRC, "1")
        .await
        .expect("release the key");
    d.settle().await;
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    assert_eq!(walk(&d, 10).await.0, []);
}

/// The plan of the first page of `agg` as it ran (`explain analyze`): which
/// ring slot tables its statement read, and which it never executed.
async fn ring_scans(d: &Driver) -> (Vec<String>, Vec<String>) {
    let plan = trellis::staging::self_check::explain_page_executed(d.pool(), "agg", None, 10)
        .await
        .expect("explain the page as run");
    let (mut read, mut skipped) = (Vec::new(), Vec::new());
    for line in plan.lines().filter(|l| l.contains("Scan on seg_")) {
        let slot = line
            .split("Scan on ")
            .nth(1)
            .and_then(|t| t.split_whitespace().next())
            .expect("a table")
            .to_string();
        if line.contains("(never executed)") {
            skipped.push(slot);
        } else {
            read.push(slot);
        }
    }
    (read, skipped)
}

/// The ring slot tables of the segments, oldest first, as `seg_N`.
async fn segment_tables(d: &Driver) -> Vec<String> {
    d.rows("select 'seg_' || ring_slot from segments order by seg_seq")
        .await
        .into_iter()
        .map(|t| t.trim_matches(|c| c == '(' || c == ')').to_string())
        .collect()
}

/// A drained slot is skipped: a page reads the ring slots that hold
/// undrained rows and never executes the scan of one whose segment is
/// drained, however many rows it still holds, for a definition with no held
/// key. A slot with no segment is skipped too.
#[tokio::test]
async fn a_drained_ring_slot_is_never_scanned_by_a_page() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 20)",
    )
    .await;
    step(&mut d, "insert into public.src values (3, 1, 5)").await;
    assert_eq!(
        d.rows("select state from segments order by seg_seq").await,
        ["(drained)", "(drained)", "(active)"],
    );
    let tables = segment_tables(&d).await;
    let (read, skipped) = ring_scans(&d).await;
    // The oldest slot's successor is drained, so its late rows were taken;
    // the unused slot has no segment. The next slot's successor is the
    // active segment, so it is skipped only when its fence saw no writer in
    // flight, which other tests running against the cluster can change.
    let unused = (0..4)
        .map(|n| format!("seg_{n}"))
        .find(|t| !tables.contains(t))
        .expect("an unused slot");
    for slot in [&tables[0], &unused] {
        assert!(skipped.contains(slot), "{slot}: {read:?} {skipped:?}");
    }
    assert!(read.contains(&tables[2]), "{read:?} {skipped:?}");
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
}

/// [`ring_scans`] with the fence of the segment before the active one set to
/// `fence` (text of a `pg_snapshot`), its `late_rows` not counted.
async fn fence_scans(d: &mut Driver, fence: &str) -> (Vec<String>, Vec<String>) {
    d.ctl
        .execute(
            &format!(
                "update segments set fence_snapshot = '{fence}'::pg_snapshot, late_rows = null \
                 where seg_seq = (select max(seg_seq) - 1 from segments)"
            ),
            &[],
        )
        .await
        .expect("set the fence");
    ring_scans(d).await
}

/// A drained slot whose fence had a writer in flight may hold that writer's
/// late rows, which the successor's window takes: it is read until the retire
/// pass has counted none, or the successor is drained; and skipped when the
/// fence had no writer in flight.
#[tokio::test]
async fn a_drained_slot_with_a_writer_in_flight_at_its_fence_is_read_until_no_late_row_is_counted()
{
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    let tables = segment_tables(&d).await;
    let before_active = tables[tables.len() - 2].clone();
    let real_fence = d
        .rows("select fence_snapshot::text from segments order by seg_seq desc offset 1 limit 1")
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    let xmax = d
        .rows("select pg_snapshot_xmax(pg_current_snapshot())")
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    // A fence every row is visible in, with and without a writer in flight.
    let (read, skipped) = fence_scans(&mut d, &format!("1:{xmax}:")).await;
    assert!(skipped.contains(&before_active), "{read:?} {skipped:?}");
    let (read, skipped) = fence_scans(&mut d, &format!("1:{xmax}:2")).await;
    assert!(read.contains(&before_active), "{read:?} {skipped:?}");
    // The retire pass counts the rows the fence doesn't show: none.
    d.retire().await;
    assert_eq!(
        d.rows("select late_rows from segments order by seg_seq desc offset 1 limit 1")
            .await,
        ["(0)"]
    );
    let (read, skipped) = ring_scans(&d).await;
    assert!(skipped.contains(&before_active), "{read:?} {skipped:?}");
    // A fence no row is visible in: the slot's rows are late rows, counted,
    // and the slot is read.
    let (read, skipped) = fence_scans(&mut d, "1:3:2").await;
    assert!(read.contains(&before_active), "{read:?} {skipped:?}");
    d.retire().await;
    assert_eq!(
        d.rows("select late_rows > 0 from segments order by seg_seq desc offset 1 limit 1")
            .await,
        ["(t)"]
    );
    let (read, skipped) = ring_scans(&d).await;
    assert!(read.contains(&before_active), "{read:?} {skipped:?}");
    // The successor drains: it took the late rows, so the slot is skipped.
    // (Under the fence the segment really had.)
    fence_scans(&mut d, &real_fence).await;
    step(&mut d, "insert into public.src values (2, 1, 1)").await;
    let (read, skipped) = ring_scans(&d).await;
    assert!(skipped.contains(&before_active), "{read:?} {skipped:?}");
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
}

/// The slot table of segment `seg_seq` and its `late_rows`, as text.
async fn slot_and_late_rows(d: &Driver, seg_seq: i64) -> (String, String) {
    let row = d
        .ctl
        .query_one(
            "select 'seg_' || ring_slot, coalesce(late_rows::text, 'uncounted') \
             from segments where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("the segment");
    (row.get(0), row.get(1))
}

/// A real late writer: a transaction that wrote into the active
/// segment and is still open when it seals leaves its row in that slot,
/// outside the fence, and the segment drains without it. The retire pass
/// doesn't count the slot's late rows while the writer is open (it could
/// still commit one); once it commits, its row is pending in a drained
/// segment's slot, so a page reads the slot and subtracts the row, and the
/// count says one late row. Once the successor drains (it applied the row),
/// the slot is skipped.
#[tokio::test]
async fn a_late_writers_row_in_a_drained_slot_is_read_until_its_successor_drains() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    let writer = d.user().await;
    writer
        .batch_execute("begin; insert into public.src values (2, 1, 5)")
        .await
        .expect("open the writer");
    let batch = d.seal().await;
    d.drain(batch, "late").await;
    assert_eq!(
        d.rows(&format!(
            "select state from segments where seg_seq = {batch}"
        ))
        .await,
        ["(drained)"]
    );
    // The writer is in the fence's in-progress list, and still open.
    d.retire().await;
    let (slot, late) = slot_and_late_rows(&d, batch).await;
    assert_eq!(late, "uncounted");
    let (read, skipped) = ring_scans(&d).await;
    assert!(read.contains(&slot), "{slot}: {read:?} {skipped:?}");
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));

    // Committed: the row is in the source and pending in the drained slot.
    writer.batch_execute("commit").await.expect("commit");
    let (read, skipped) = ring_scans(&d).await;
    assert!(read.contains(&slot), "{slot}: {read:?} {skipped:?}");
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    d.retire().await;
    assert_eq!(slot_and_late_rows(&d, batch).await.1, "1");
    let (read, skipped) = ring_scans(&d).await;
    assert!(read.contains(&slot), "{slot}: {read:?} {skipped:?}");
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));

    // The successor takes the row and drains.
    let next = d.seal().await;
    d.drain(next, "successor").await;
    assert_eq!(
        slot_and_late_rows(&d, batch).await.0,
        slot,
        "not retired yet"
    );
    let (read, skipped) = ring_scans(&d).await;
    assert!(skipped.contains(&slot), "{slot}: {read:?} {skipped:?}");
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    assert_eq!(
        d.rows("select total from public.agg where g = 1").await,
        ["(15)"]
    );
}

/// A definition with a held key reads the drained slots: the held key's
/// rows are still subtracted from the source, in a drained segment too.
#[tokio::test]
async fn a_drained_ring_slot_is_read_for_a_definition_with_a_held_key() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 20)",
    )
    .await;
    step(&mut d, "insert into public.src values (3, 1, 5)").await;
    let oldest = segment_tables(&d).await[0].clone();
    assert!(ring_scans(&d).await.1.contains(&oldest));
    d.ctl
        .execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, $1, '1', 'test' from transform_definitions \
             where target_table = 'public.agg'",
            &[&SRC],
        )
        .await
        .expect("hold the key");
    step(&mut d, "update public.src set g = 2, v = 11 where id = 1").await;
    step(&mut d, "update public.src set v = 12 where id = 1").await;
    assert_eq!(d.rows("select count(*) from poison_held").await, ["(1)"]);
    let oldest = segment_tables(&d).await[0].clone();
    let (read, skipped) = ring_scans(&d).await;
    assert!(
        skipped.is_empty() && read.contains(&oldest),
        "{read:?} {skipped:?}"
    );
    // The parked changes sit in drained segments, and are subtracted.
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
}

/// A source `TRUNCATE` the drain hasn't applied touches every group: the
/// comparison can't vouch for any until it applies.
#[tokio::test]
async fn a_pending_truncate_leaves_every_group_uncertain() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 2, 20)",
    )
    .await;
    exec(&d, "truncate public.src").await;
    let (_, _, all) = compare(&d, 10).await;
    assert!(all);
    d.settle().await;
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
}

/// A group an absolute write (a recompute, a rebuild) rewrote from a
/// snapshot that saw a change the drain hasn't applied yet already counts
/// that change, and its page will refuse it: the comparison doesn't
/// subtract it either. A change after the snapshot is subtracted as usual.
#[tokio::test]
async fn a_pending_change_a_groups_basis_saw_is_not_subtracted() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 1, 5), (3, 2, 7)",
    )
    .await;
    exec(&d, "insert into public.src values (4, 1, 100)").await;
    let batch = d.seal().await;
    let hidden = hidden(&d).await;
    exec(
        &d,
        &format!(
            "update {hidden} set __n = 3, __count_1 = 3, __sum_1 = 115, \
                 __basis = pg_current_snapshot() where g = 1; \
             update public.agg set total = 115, n = 3 where g = 1"
        ),
    )
    .await;
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    exec(&d, "insert into public.src values (5, 1, 1)").await;
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    d.drain(batch, "refuses").await;
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    d.settle().await;
    assert_eq!(walk(&d, 10).await.0, []);
}

/// A change from before a source `TRUNCATE` that reaches no page until after
/// the truncate's page raised the truncate floor will be refused by that
/// floor, so the target never counts it: the comparison doesn't subtract it
/// either. The driver drains the two segments out of order, which the
/// barrier never does; the floor is what refuses it.
#[tokio::test]
async fn a_pending_change_at_or_below_the_truncate_floor_is_not_subtracted() {
    let mut d = start().await;
    step(&mut d, "insert into public.src values (1, 1, 10)").await;
    exec(&d, "insert into public.src values (2, 1, 5), (3, 2, 7)").await;
    let before = d.seal().await;
    exec(
        &d,
        "truncate public.src; insert into public.src values (4, 2, 1)",
    )
    .await;
    let truncate = d.seal().await;
    d.drain(truncate, "a").await;
    assert_eq!(
        d.rows("select count(*) from ledger_truncate_floor where target_table = 'public.agg'")
            .await,
        ["(1)"],
        "the truncate raised the floor"
    );
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    d.drain(before, "b").await;
    assert_eq!(compare(&d, 10).await, (vec![], vec![], false));
    assert_eq!(walk(&d, 10).await.0, []);
}

/// Groups page in the order of their key's type, `NULL` last, not of its
/// text (`10` after `9`): a walk at every page size compares each group once
/// and finds nothing, and the cursor never skips or repeats one.
#[tokio::test]
async fn groups_page_in_their_key_order_with_the_null_group_last() {
    let mut d = start().await;
    let mut values = Vec::new();
    for g in [1, 2, 3, 9, 10, 11, 20, 100] {
        values.push(format!("({g}, {g}, {g})"));
    }
    values.push("(200, null, 5)".to_string());
    step(
        &mut d,
        &format!("insert into public.src values {}", values.join(", ")),
    )
    .await;
    for limit in [1, 2, 3, 4, 100] {
        let (found, compared, pages) = walk(&d, limit).await;
        assert_eq!(found, [], "limit {limit}");
        assert_eq!(compared, 9, "every group once at limit {limit}");
        assert!(pages >= 1);
    }
    // A planted divergence on the group after the `9` boundary is reported
    // once, in the numeric order.
    let hidden = hidden(&d).await;
    exec(
        &d,
        &format!("update {hidden} set __n = __n + 1 where g = 10"),
    )
    .await;
    let (found, _, _) = walk(&d, 3).await;
    assert!(!found.is_empty(), "{found:?}");
    assert!(
        found
            .iter()
            .all(|d| matches!(d, Divergence::Cell { key, .. } if key == "10")),
        "{found:?}"
    );
}

/// A page of an aggregate's source is a range scan of an index that leads
/// with the group columns; without one it is a scan of the source (the
/// documented cost).
#[tokio::test]
async fn an_aggregate_page_reads_its_source_by_an_index_that_leads_with_the_group_column() {
    let d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
         SELECT SUM(v) AS total, COUNT(*) AS n",
        "create table public.src (id integer primary key, g integer, v integer)",
        &[("id", int4()), ("g", int4()), ("v", int4())],
    )
    .await;
    let explain = |d: &Driver| {
        let pool = d.pool().clone();
        async move {
            trellis::staging::self_check::explain_page(&pool, "agg", Some("5"), 3)
                .await
                .expect("explain the page")
        }
    };
    let src_seq_scans = |plan: &str| {
        plan.lines()
            .filter(|l| l.contains("Seq Scan on src "))
            .count()
    };
    let without = explain(&d).await;
    assert!(!without.contains("src_g_idx"), "{without}");
    exec(&d, "create index src_g_idx on public.src (g)").await;
    let with = explain(&d).await;
    assert!(
        with.contains("Index Scan using src_g_idx on src"),
        "the group page is a range scan of the index:\n{with}"
    );
    // Past the cursor the page reads the groups above it and, apart, the
    // `NULL` group, each by a range of the index. One scan of the source is
    // left: the branch for image-less ring rows, which reads it only when such
    // a row exists.
    assert!(with.contains("Index Cond: (g > 5)"), "{with}");
    assert!(with.contains("Index Cond: (g IS NULL)"), "{with}");
    assert_eq!(src_seq_scans(&with), 1, "{with}");
    assert!(src_seq_scans(&without) > 1, "{without}");
    // The hidden table and the visible target are read by their keys' indexes.
    assert!(
        with.contains("Index Scan using agg_1_g_key on agg_1"),
        "{with}"
    );
    assert!(with.contains("Index Scan using agg_g_key on agg"), "{with}");
}

/// A composite group key's page is a range scan of an index that leads with
/// the group columns in `GROUP BY` order, with or without trailing columns,
/// read forward or, all descending with `NULL` first, backward.
/// Define refuses any other order (`tests/source_index.rs`).
#[tokio::test]
async fn a_composite_page_is_index_served_by_an_index_in_group_by_order() {
    let d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g, h WITHOUT SOURCE INDEX \
         SELECT SUM(v) AS total, COUNT(*) AS n",
        "create table public.src (id integer primary key, g integer, h integer, v integer)",
        &[("id", int4()), ("g", int4()), ("h", int4()), ("v", int4())],
    )
    .await;
    for (name, columns) in [
        ("src_gh", "g, h"),
        ("src_ghv", "g, h, v"),
        ("src_back", "g desc, h desc"),
    ] {
        exec(
            &d,
            &format!("create index {name} on public.src ({columns})"),
        )
        .await;
        let pool = d.pool().clone();
        let plan = trellis::staging::self_check::explain_page(&pool, "agg", Some("5\u{1f}5"), 3)
            .await
            .expect("explain the page");
        assert!(
            plan.contains(&format!("Scan using {name} on src s"))
                || plan.contains(&format!("Scan Backward using {name} on src s")),
            "index ({columns}):\n{plan}"
        );
        assert!(
            plan.contains("Index Cond: (ROW(g, h) > ROW(5, 5))"),
            "index ({columns}):\n{plan}"
        );
        assert!(
            !plan.contains("HashAggregate"),
            "index ({columns}):\n{plan}"
        );
        exec(&d, &format!("drop index public.{name}")).await;
    }
}

/// A two-column nullable group key, `NULL` in either part, against pending
/// changes that add groups only the ring knows (a deleted group's rows, a
/// new group's), with and without an index leading with the group columns
/// (the ordered, unbounded page): at every page size the
/// comparison finds nothing, and a group planted on one side only is
/// reported alike.
#[tokio::test]
async fn nullable_composite_groups_with_ring_only_keys_page_alike_with_and_without_an_index() {
    let mut d = start_with(
        "TRANSFORM agg FROM public.src GROUP BY g, h WITHOUT SOURCE INDEX \
         SELECT SUM(v) AS total, COUNT(*) AS n",
        "create table public.src (id integer primary key, g integer, h integer, v integer)",
        &[("id", int4()), ("g", int4()), ("h", int4()), ("v", int4())],
    )
    .await;
    let mut rows = Vec::new();
    let mut id = 0;
    for g in ["null", "1", "2", "10"] {
        for h in ["null", "1", "9", "10"] {
            id += 1;
            rows.push(format!("({id}, {g}, {h}, {id})"));
        }
    }
    step(
        &mut d,
        &format!("insert into public.src values {}", rows.join(", ")),
    )
    .await;
    // Pending: a group emptied (only the ring still holds its rows), a new
    // group (only in the source and the ring), and a row moved into the
    // `NULL` region.
    exec(
        &d,
        "delete from public.src where g = 2 and h = 9; \
         insert into public.src values (100, 3, null, 1), (101, null, 5, 2); \
         update public.src set g = null, h = null where id = 6",
    )
    .await;
    for indexed in [false, true] {
        if indexed {
            exec(&d, "create index src_g_h_idx on public.src (g, h)").await;
        }
        for limit in [1, 2, 3, 4, 100] {
            assert_eq!(
                compare(&d, limit).await,
                (vec![], vec![], false),
                "indexed {indexed}, limit {limit}"
            );
        }
        // A group only the target has, between two in the `NULL` region.
        exec(
            &d,
            "insert into public.agg (g, h, total, n) values (null, 7, 1, 1)",
        )
        .await;
        for limit in [1, 2, 3, 4, 100] {
            let (found, uncertain, all) = compare(&d, limit).await;
            assert_eq!(
                found,
                [Divergence::ExtraRow {
                    key: "\u{1}\u{1f}7".into()
                }],
                "indexed {indexed}, limit {limit}"
            );
            assert_eq!((uncertain, all), (vec![], false));
        }
        exec(&d, "delete from public.agg where g is null and h = 7").await;
    }
}

const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 1_000,
    drain_batch_cap: usize::MAX,
    heartbeat_interval: Duration::from_secs(5),
    reclaim_ttl: Duration::from_secs(60),
};

/// `Trellis::request_backfill` of the source, then the rebuild's one sweep
/// job claimed and run by hand to its commit (as `group_basis_rebuild.rs`).
async fn request_backfill_and_rebuild(d: &Driver) {
    let trellis = Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    trellis
        .request_backfill("src")
        .await
        .expect("request a rebuild");
    d.run_frozen(&[], |pool| async move {
        let claimed = chunk_queue::claim_chunks_of(
            &**pool.get().await.expect("pool"),
            "rebuild",
            1,
            &[chunk_queue::KIND_SWEEP],
        )
        .await
        .expect("claim the rebuild");
        assert_eq!(claimed.len(), 1, "one rebuild job to claim");
        build::run_claimed(&pool, &claimed[0], "rebuild", &OPTIONS).await;
        Ok(())
    })
    .await
    .finish()
    .await;
    assert_eq!(
        d.rows(
            "select status, (select count(*) from backfill_chunks) \
             from transform_definitions where target_table = 'public.agg'"
        )
        .await,
        ["(live,0)"],
        "the rebuild committed and flipped the definition live"
    );
}

/// The acceptance test end to end: planted damage of every kind (a hand-edited
/// partial, a hand-edited visible row, a group missing from both tables, a
/// visible row of no group, and a write made while capture was off, which
/// no undo of a plant could repair) is reported by the check, the user's
/// `request_backfill` rebuilds the groups in place, and the check is then clean
/// over every group.
#[tokio::test]
async fn a_planted_divergence_is_reported_until_request_backfill_repairs_it() {
    let mut d = start().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 10), (2, 1, 5), (3, 2, 7), (4, 3, 1), (5, 4, 2)",
    )
    .await;
    assert_eq!(walk(&d, 100).await.0, [], "converged before the plant");
    let hidden = hidden(&d).await;
    let name = hidden.rsplit('.').next().unwrap().to_string();

    exec(
        &d,
        &format!(
            "update {hidden} set __sum_1 = __sum_1 + 1000 where g = 1; \
             update public.agg set total = 99 where g = 2; \
             delete from {hidden} where g = 3; delete from public.agg where g = 3; \
             insert into public.agg (g, total, n) values (42, 1, 1)"
        ),
    )
    .await;
    exec(
        &d,
        "alter table public.src disable trigger trellis_capture_insert",
    )
    .await;
    exec(&d, "insert into public.src values (6, 4, 100), (7, 7, 1)").await;
    exec(
        &d,
        "alter table public.src enable always trigger trellis_capture_insert",
    )
    .await;
    d.settle().await;

    let (found, _, _) = walk(&d, 100).await;
    let expected = [
        // g = 1: the hand-edited partial, and the visible row it no longer matches.
        cell("1", &format!("{name}.__sum_1"), Some("1015"), Some("15")),
        cell("1", "total", Some("15"), Some("1015")),
        // g = 2: the hand-edited visible row.
        cell("2", "total", Some("99"), Some("7")),
        // g = 4: the capture-off write.
        cell("4", &format!("{name}.__sum_1"), Some("2"), Some("102")),
    ];
    for want in &expected {
        assert!(found.contains(want), "missing {want:?} in {found:?}");
    }
    for want in [
        Divergence::MissingRow { key: "3".into() },
        Divergence::MissingRow { key: "7".into() },
        Divergence::ExtraRow { key: "42".into() },
    ] {
        assert!(found.contains(&want), "missing {want:?} in {found:?}");
    }
    // Nothing else: group 1 (partial, visible row), group 2 (visible row), groups 3
    // and 7 (three partials each, and the missing row), group 4 (three
    // partials), and the extra row of 42.
    assert_eq!(found.len(), 15, "{found:?}");

    request_backfill_and_rebuild(&d).await;

    let (found, compared, _) = walk(&d, 100).await;
    assert_eq!(found, [], "the rebuild repaired every plant");
    let groups = d.rows(&format!("select count(*) from {hidden}")).await;
    assert_eq!(
        groups,
        [format!("({compared})")],
        "every group the rebuild left was compared"
    );
    // The same walk, a page at a time.
    assert_eq!(walk(&d, 1).await.0, []);
}
