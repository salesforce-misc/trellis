//! `self_check` of an aggregate target on the group basis (#1104, ADR-0013
//! "Scope"). It detects and never repairs: a planted divergence is reported,
//! and gone once the planted damage is undone (the repair for real damage,
//! `request_backfill` of the source, is the group-basis build's, #1101).
//!
//! Every test turns the group-basis dev knob on before its driver starts,
//! drives the pipeline by hand through `support/drain_driver.rs` and never
//! polls for convergence (#297): a check either runs on a drained pipeline or
//! meets a sealed page nobody drained.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page,
};

const SRC: &str = "public.src";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn start_with(definition: &str, source_ddl: &str, columns: &[(&str, ValueType)]) -> Driver {
    trellis::dev::staging::set_group_basis(true);
    Driver::start(source_ddl, columns, &[definition], &[SRC]).await
}

async fn start() -> Driver {
    start_with(
        AGG,
        "create table public.src (id integer primary key, g integer, v integer)",
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
         SELECT SUM(m) AS total, AVG(m) AS mean, SUM(a) + COUNT(m) AS mixed, COUNT(*) AS n",
        "create table public.src (id integer primary key, g integer, a integer, m numeric(10, 2))",
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
         SELECT SUM(a + b) AS ab, COUNT(a + b) AS nab, COALESCE(SUM(b), 0) AS sb, \
                SUM(a) > COUNT(b) AS apart, h + COUNT(*) AS hp",
        "create table public.src (id integer primary key, g integer, h integer, \
                                  a integer, b integer)",
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
