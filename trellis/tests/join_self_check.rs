//! `self_check` of a `JOIN` target (#1112 J7a, ADR-0013 "Scope", ADR-0019).
//! It detects and never repairs: the visible target is compared against the
//! definition's own `JOIN … GROUP BY`, and the hidden `P`, `M`, `J` and `T`
//! against what the two tables say they hold. A planted divergence in each is
//! reported on that table alone, and gone once the planted damage is undone
//! by hand (the repair for real damage, `request_backfill`, needs the `JOIN`
//! build, #1109).
//!
//! Every test turns the group-basis dev knob on before its driver starts (a
//! `JOIN` applies only behind it), drives the pipeline by hand through
//! `support/drain_driver.rs` and never polls for convergence (#297).

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

const DDL: &str = "create table public.a (id integer primary key, ga integer, t integer, \
                       k integer, x integer); \
                   create table public.b (id integer primary key, t integer, k integer, \
                       gb integer, y integer)";

/// A `LEFT JOIN` on the pair `(t, k)`, grouped by a column of each side,
/// with an aggregate of each kind on each side, a composed field and an
/// argument only counted (whose hidden sum isn't kept).
const LEFT_PAIR: &str = "TRANSFORM l FROM public.a LEFT JOIN public.b \
     ON a.t = b.t AND a.k = b.k GROUP BY a.ga, b.gb \
     SELECT a.ga AS ga, b.gb AS gb, COUNT(*) AS n, COUNT(a.x) AS cx, SUM(a.x) AS sx, \
            AVG(b.y) AS ay, COALESCE(SUM(b.y), 0) + COUNT(*) AS mixed, COUNT(b.y) AS cy, \
            COUNT(a.t) AS ct";

/// An inner `JOIN` on `k`, grouped by the `FROM` side only: its target's
/// key is `ga`'s text.
const INNER: &str = "TRANSFORM i FROM public.a JOIN public.b ON a.k = b.k GROUP BY a.ga \
     SELECT a.ga AS ga, COUNT(*) AS n, SUM(a.x) AS sx, SUM(b.y) AS sy";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn start(definitions: &[&str]) -> Driver {
    trellis::dev::staging::set_group_basis(true);
    Driver::start_with_database_defaults(
        &[],
        DDL,
        &[
            ("id", int4()),
            ("ga", int4()),
            ("t", int4()),
            ("k", int4()),
            ("x", int4()),
        ],
        &[],
        definitions,
        &["public.a", "public.b"],
    )
    .await
}

async fn exec(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

async fn step(d: &mut Driver, sql: &str) {
    exec(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

/// The rows every test starts from: matched and unmatched keys on both
/// sides, a `NULL` in either part of the join key on either side, `NULL`
/// group values and arguments, a cell that emptied and a key that lost its
/// last match.
async fn seed(d: &mut Driver) {
    for sql in [
        "insert into public.a values (1, 1, 1, 1, 10), (2, 1, 1, 1, null), (3, 2, 1, 2, 5), \
                                     (4, null, 1, 3, 7), (5, 2, null, 1, 1), (6, 3, 1, null, 2), \
                                     (7, 4, 2, 9, 3)",
        "insert into public.b values (1, 1, 1, 10, 100), (2, 1, 1, 20, null), (3, 1, 2, null, 4), \
                                     (4, 1, null, 10, 8), (5, null, 1, 30, 9), (6, 2, 9, 40, 1), \
                                     (7, 1, 3, 50, 6)",
        "update public.a set ga = 3 where id = 3",
        "delete from public.b where id = 6",
        "update public.b set gb = 11 where id = 1",
        "delete from public.a where id = 4",
    ] {
        step(d, sql).await;
    }
}

/// Definition `target`'s hidden table `part` (`p`, `m`, `j`, `t`): its
/// qualified name, and its bare name as a divergence names it.
async fn hidden(d: &Driver, target: &str, part: &str) -> (String, String) {
    let id = d
        .rows(&format!(
            "select id from transform_definitions where target_table = 'public.{target}'"
        ))
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    let name = format!("join_{id}_{part}");
    (format!("{DEFAULT_SCHEMA}.{name}"), name)
}

/// Every page of the check of `target`, `limit` keys at a time: the
/// divergences, the keys compared and the number of pages.
async fn walk(d: &Driver, target: &str, limit: i64) -> (Vec<Divergence>, i64, usize) {
    let mut after = None;
    let (mut found, mut compared, mut pages) = (Vec::new(), 0, 0);
    loop {
        let page = check_page(
            d.pool(),
            target,
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

/// The one divergence `found` holds, a cell of `column`, with its values.
fn only_cell(found: &[Divergence], column: &str, persisted: &str, recomputed: &str) {
    assert!(
        matches!(found, [Divergence::Cell { column: c, persisted: p, recomputed: r, .. }]
            if c == column && p.as_deref() == Some(persisted)
                && r.as_deref() == Some(recomputed)),
        "expected only {column}: {persisted} (recomputed {recomputed}), got {found:?}"
    );
}

/// A converged `JOIN` target reports nothing, at every page size, on both
/// kinds; every part is walked (the keys compared count `T`'s groups and
/// the visible rows, and `P`'s, `M`'s and `J`'s keys).
#[tokio::test]
async fn a_converged_join_target_reports_no_divergence_at_every_page_size() {
    let mut d = start(&[LEFT_PAIR, INNER]).await;
    seed(&mut d).await;
    for target in ["l", "i"] {
        let mut counts = Vec::new();
        for limit in [1, 2, 3, 100] {
            let (found, compared, pages) = walk(&d, target, limit).await;
            assert_eq!(found, [], "{target}, limit {limit}");
            counts.push(compared);
            if limit == 1 {
                assert!(pages > 10, "{target}: paged by keys in each part: {pages}");
            } else if limit == 100 {
                assert_eq!(pages, 4, "{target}: one page a part");
            }
        }
        assert!(
            counts.iter().all(|c| *c == counts[0]),
            "{target}: every key is reached at every page size: {counts:?}"
        );
    }
}

/// A sealed page nobody has drained yet is "not caught up", never a
/// divergence.
#[tokio::test]
async fn a_page_still_to_apply_is_not_caught_up_rather_than_diverged() {
    let mut d = start(&[INNER]).await;
    step(&mut d, "insert into public.a values (1, 1, 1, 1, 10)").await;
    exec(&d, "insert into public.b values (1, 1, 1, 10, 100)").await;
    let batch = d.seal().await;
    let page = check_page(
        d.pool(),
        "i",
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
    assert_eq!(walk(&d, "i", 10).await.0, []);
}

/// A hand-edited `P` cell is reported on `P` alone (`T` and the target are
/// compared against the tables, not against `P`), and gone once restored.
#[tokio::test]
async fn a_planted_divergence_in_p_is_reported_until_it_is_undone() {
    let mut d = start(&[LEFT_PAIR]).await;
    seed(&mut d).await;
    let (p, name) = hidden(&d, "l", "p").await;
    for (set, column, persisted, recomputed) in [
        ("__n = __n + 1", "__n", "3", "2"),
        ("__count_1 = __count_1 + 1", "__count_1", "2", "1"),
        ("__sum_1 = __sum_1 + 1", "__sum_1", "11", "10"),
    ] {
        exec(
            &d,
            &format!("update {p} set {set} where ga = 1 and t = 1 and k = 1"),
        )
        .await;
        for limit in [1, 100] {
            only_cell(
                &walk(&d, "l", limit).await.0,
                &format!("{name}.{column}"),
                persisted,
                recomputed,
            );
        }
        let undo = set.replace('+', "-");
        exec(
            &d,
            &format!("update {p} set {undo} where ga = 1 and t = 1 and k = 1"),
        )
        .await;
        assert_eq!(walk(&d, "l", 2).await.0, [], "after undoing {set}");
    }
}

/// The same for `M`, the joined side's cells, on a cell of the `NULL`
/// group.
#[tokio::test]
async fn a_planted_divergence_in_m_is_reported_until_it_is_undone() {
    let mut d = start(&[LEFT_PAIR]).await;
    seed(&mut d).await;
    let (m, name) = hidden(&d, "l", "m").await;
    for (set, column, persisted, recomputed) in [
        ("__n = __n + 1", "__n", "2", "1"),
        ("__count_1 = __count_1 + 1", "__count_1", "2", "1"),
        ("__sum_1 = __sum_1 + 1", "__sum_1", "5", "4"),
    ] {
        exec(
            &d,
            &format!("update {m} set {set} where gb is null and t = 1 and k = 2"),
        )
        .await;
        for limit in [1, 100] {
            only_cell(
                &walk(&d, "l", limit).await.0,
                &format!("{name}.{column}"),
                persisted,
                recomputed,
            );
        }
        let undo = set.replace('+', "-");
        exec(
            &d,
            &format!("update {m} set {undo} where gb is null and t = 1 and k = 2"),
        )
        .await;
        assert_eq!(walk(&d, "l", 2).await.0, [], "after undoing {set}");
    }
}

/// A `J` row whose match count is off, and one deleted under a matched
/// key, are reported on `J` alone; a stray `J` row of no match is not a
/// divergence (`J` keeps every key a page named).
#[tokio::test]
async fn a_planted_divergence_in_j_is_reported_until_it_is_undone() {
    let mut d = start(&[INNER]).await;
    seed(&mut d).await;
    let (j, name) = hidden(&d, "i", "j").await;
    let column = format!("{name}.__matches");

    exec(&d, &format!("update {j} set __matches = 7 where k = 1")).await;
    // Key 1: b's rows 1, 2 and 5 (row 5's `NULL` `t` isn't part of this
    // key; row 4's `NULL` `k` joins nothing).
    assert_eq!(
        walk(&d, "i", 1).await.0,
        [cell("1", &column, Some("7"), Some("3"))]
    );
    exec(&d, &format!("update {j} set __matches = 3 where k = 1")).await;
    assert_eq!(walk(&d, "i", 2).await.0, []);

    exec(&d, &format!("delete from {j} where k = 2")).await;
    assert_eq!(
        walk(&d, "i", 100).await.0,
        [cell("2", &column, Some("0"), Some("1"))]
    );
    exec(&d, &format!("insert into {j} (k, __matches) values (2, 1)")).await;
    assert_eq!(walk(&d, "i", 2).await.0, []);

    exec(
        &d,
        &format!("insert into {j} (k, __matches) values (77, 0)"),
    )
    .await;
    assert_eq!(walk(&d, "i", 2).await.0, []);
}

/// A hand-edited `T` partial is reported on `T` alone: the visible row
/// still equals the query, since the target is compared against the query,
/// not against `T`.
#[tokio::test]
async fn a_planted_divergence_in_t_is_reported_until_it_is_undone() {
    let mut d = start(&[INNER]).await;
    seed(&mut d).await;
    let (t, name) = hidden(&d, "i", "t").await;
    // Group 1: a's rows 1 and 2 (key 1), each matching b's rows 1, 2 and
    // 5. `T`'s arguments: a.x first, then b.y.
    for (set, column, persisted, recomputed) in [
        ("__n = __n + 1", "__n", "7", "6"),
        ("__count_1 = __count_1 + 1", "__count_1", "4", "3"),
        ("__sum_1 = __sum_1 + 1", "__sum_1", "31", "30"),
        ("__count_2 = __count_2 + 1", "__count_2", "5", "4"),
        ("__sum_2 = __sum_2 + 1", "__sum_2", "219", "218"),
    ] {
        exec(&d, &format!("update {t} set {set} where ga = 1")).await;
        for limit in [1, 100] {
            assert_eq!(
                walk(&d, "i", limit).await.0,
                [cell(
                    "1",
                    &format!("{name}.{column}"),
                    Some(persisted),
                    Some(recomputed)
                )],
                "{set}, limit {limit}"
            );
        }
        let undo = set.replace('+', "-");
        exec(&d, &format!("update {t} set {undo} where ga = 1")).await;
        assert_eq!(walk(&d, "i", 2).await.0, [], "after undoing {set}");
    }
}

/// The visible target against the query: a wrong value, a missing row and
/// a row of no group, each on its own key, and each gone once undone.
#[tokio::test]
async fn a_visible_row_wrong_missing_or_extra_is_reported() {
    let mut d = start(&[INNER]).await;
    seed(&mut d).await;
    assert_eq!(
        d.rows("select ga, n, sx, sy from public.i where ga = 2")
            .await,
        ["(2,3,3,109)"]
    );

    exec(&d, "update public.i set sy = 99 where ga = 1").await;
    exec(&d, "delete from public.i where ga = 2").await;
    exec(
        &d,
        "insert into public.i (ga, n, sx, sy) values (8, 1, 1, 1)",
    )
    .await;
    for limit in [1, 2, 100] {
        let found = walk(&d, "i", limit).await.0;
        assert_eq!(
            found,
            [
                cell("1", "sy", Some("99"), Some("218")),
                Divergence::MissingRow { key: "2".into() },
                Divergence::ExtraRow { key: "8".into() },
            ],
            "limit {limit}"
        );
    }

    exec(&d, "update public.i set sy = 218 where ga = 1").await;
    exec(&d, "delete from public.i where ga = 8").await;
    exec(
        &d,
        "insert into public.i (ga, n, sx, sy) values (2, 3, 3, 109)",
    )
    .await;
    assert_eq!(walk(&d, "i", 2).await.0, []);
}

/// A write capture never saw leaves `P`, `J`, `T` and the target stale by
/// it; each is reported, on the key it touched.
#[tokio::test]
async fn a_write_made_while_capture_was_off_is_reported_on_every_structure() {
    let mut d = start(&[INNER]).await;
    seed(&mut d).await;
    for table in ["a", "b"] {
        exec(
            &d,
            &format!("alter table public.{table} disable trigger trellis_capture_insert"),
        )
        .await;
    }
    exec(&d, "insert into public.b values (20, 1, 3, 5, 1)").await;
    exec(&d, "insert into public.a values (20, 6, 1, 3, 1)").await;
    for table in ["a", "b"] {
        exec(
            &d,
            &format!("alter table public.{table} enable always trigger trellis_capture_insert"),
        )
        .await;
    }
    d.settle().await;

    let found = walk(&d, "i", 3).await.0;
    let (_, p) = hidden(&d, "i", "p").await;
    let (_, m) = hidden(&d, "i", "m").await;
    let (_, j) = hidden(&d, "i", "j").await;
    let (_, t) = hidden(&d, "i", "t").await;
    for table in [&p, &m, &j, &t] {
        assert!(
            found
                .iter()
                .any(|d| matches!(d, Divergence::Cell { column, .. }
                    if column.starts_with(&format!("{table}.")))),
            "{table}: {found:?}"
        );
    }
    assert!(
        found.contains(&cell("3", &format!("{j}.__matches"), Some("1"), Some("2"))),
        "{found:?}"
    );
    assert!(
        found.contains(&Divergence::MissingRow { key: "6".into() }),
        "{found:?}"
    );
}

/// Whichever side fills a page first bounds it: a group only the target
/// has, or only the tables have, sorting between the others' groups (`"15"`
/// between `"1"` and `"2"`) is reported alike at every page size, and no
/// group past a side's cut is read as missing on it.
#[tokio::test]
async fn a_group_on_one_side_only_is_reported_alike_at_every_page_size() {
    let mut d = start(&[INNER]).await;
    seed(&mut d).await;
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
    exec(
        &d,
        "insert into public.i (ga, n, sx, sy) values (15, 1, 1, 1)",
    )
    .await;
    let mut found = Vec::new();
    for limit in [1, 2, 3, 100] {
        found.push(walk(&d, "i", limit).await.0);
    }
    assert_eq!(
        same_at_every_limit(found),
        [Divergence::ExtraRow { key: "15".into() }]
    );
    exec(&d, "delete from public.i where ga = 15").await;

    // Only the tables have group 15.
    exec(
        &d,
        "alter table public.a disable trigger trellis_capture_insert",
    )
    .await;
    exec(&d, "insert into public.a values (30, 15, 1, 1, 2)").await;
    exec(
        &d,
        "alter table public.a enable always trigger trellis_capture_insert",
    )
    .await;
    d.settle().await;
    let mut found = Vec::new();
    for limit in [1, 2, 3, 100] {
        found.push(walk(&d, "i", limit).await.0);
    }
    let found = same_at_every_limit(found);
    assert!(
        found.contains(&Divergence::MissingRow { key: "15".into() }),
        "{found:?}"
    );
    assert_eq!(
        found.len(),
        9,
        "T's five partials, the row, and P's three partials: {found:?}"
    );
}
