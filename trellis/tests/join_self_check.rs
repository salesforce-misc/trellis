//! `self_check` of a `JOIN` target (#1112 J7a, ADR-0013 "Scope", ADR-0019).
//! It detects and never repairs: the visible target is compared against the
//! definition's own `JOIN … GROUP BY`, and the hidden `P`, `M`, `J` and `T`
//! against what the two tables say they hold. A planted divergence in each is
//! reported on that table alone, and gone once the planted damage is undone
//! by hand. The last tests here run the loop, check, repair, check: writes
//! capture missed repaired by `request_backfill`, hand edits of `J`/`T` by
//! `PAUSE`/`RESUME`. One test here checks the `J` lock-wait
//! histogram a page records (#1112 J7b), since it needs the same knob-on
//! driver.
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
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};
use trellis::{Config, Trellis, TrellisOptions};

const DDL: &str = "create table public.a (id integer primary key, ga integer, t integer, \
                       k integer, x integer); \
                   create table public.b (id integer primary key, t integer, k integer, \
                       gb integer, y integer); \
                   create index on public.a (t, k); create index on public.a (k); \
                   create index on public.b (t, k); create index on public.b (k)";

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

/// Each `JOIN` page records its `J` lock statement's time under the
/// transform's label, one observation per page that applies it.
#[tokio::test]
async fn each_join_page_records_its_j_lock_wait() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start_with_database_defaults(
        &[],
        "create table public.a (id integer primary key, ga integer, k integer); \
         create table public.b (id integer primary key, k integer, y integer); \
         create index on public.a (k); create index on public.b (k)",
        &[("id", int4()), ("ga", int4()), ("k", int4())],
        &[],
        &[
            "TRANSFORM lock_wait_join FROM public.a JOIN public.b ON a.k = b.k GROUP BY a.ga \
           SELECT a.ga AS ga, COUNT(*) AS n, SUM(b.y) AS sy",
        ],
        &["public.a", "public.b"],
    )
    .await;
    let count = || {
        let rendered = trellis::metrics::Metrics::new().render_prometheus();
        rendered
            .lines()
            .find(|line| {
                line.starts_with("trellis_join_lock_wait_seconds_count{")
                    && line.contains("lock_wait_join")
            })
            .map(|line| line.rsplit(' ').next().unwrap().parse::<f64>().unwrap())
    };
    assert_eq!(count(), None);
    for sql in [
        "insert into public.a values (1, 1, 7)",
        "insert into public.b values (1, 7, 10)",
    ] {
        d.user().await.batch_execute(sql).await.expect(sql);
        let batch = d.seal().await;
        d.drain(batch, "step").await;
    }
    assert_eq!(count(), Some(2.0));
}

/// The capture audit covers both tables of a `JOIN` (#1110): capture
/// triggers that aren't `ENABLE ALWAYS` on either table are reported as
/// capture faults on that table, before any comparison, and nothing once
/// they are again.
#[tokio::test]
async fn a_capture_fault_on_either_table_is_reported_before_the_comparison() {
    let mut d = start(&[INNER]).await;
    seed(&mut d).await;
    for table in ["public.a", "public.b"] {
        d.ctl
            .batch_execute(&format!("alter table {table} disable trigger user"))
            .await
            .expect("disable capture");
        let (found, compared, _) = walk(&d, "i", 100).await;
        assert_eq!(compared, 0, "{table}: nothing is compared");
        assert!(
            !found.is_empty()
                && found.iter().all(|f| matches!(
                    f,
                    Divergence::Capture(fault) if fault.table() == Some(table)
                )),
            "{table}: {found:?}"
        );
        d.ctl
            .batch_execute(&format!(
                "do $$ declare t text; begin \
                   for t in select tgname from pg_catalog.pg_trigger \
                            where tgrelid = '{table}'::regclass and not tgisinternal loop \
                     execute format('alter table {table} enable always trigger %I', t); \
                   end loop; end $$"
            ))
            .await
            .expect("enable capture");
        assert_eq!(walk(&d, "i", 100).await.0, Vec::new(), "{table}");
    }
}

// ------------------------------------- the ring rows not applied yet (#1140)

/// One comparison of every part of `target`, `limit` keys a page, with no
/// convergence wait and no re-check, whatever the ring holds: the
/// divergences, and per part (`t`, `p`, `m`, `j`) the keys it couldn't vouch
/// for exactly, `*` for every key.
async fn compare(d: &Driver, target: &str, limit: i64) -> (Vec<Divergence>, Vec<String>) {
    let mut after: Option<String> = None;
    let (mut found, mut uncertain) = (Vec::new(), Vec::new());
    loop {
        let part = after
            .as_deref()
            .map_or("t", |a| a.split(':').next().unwrap())
            .to_string();
        let page = compare_page(d.pool(), target, SelfCheckScope { after, limit })
            .await
            .expect("a comparison");
        found.extend(page.divergences);
        if page.uncertain_all {
            uncertain.push(format!("{part}:*"));
        }
        uncertain.extend(page.uncertain_keys.iter().map(|k| format!("{part}:{k}")));
        after = page.next_after;
        if after.is_none() {
            uncertain.sort();
            uncertain.dedup();
            return (found, uncertain);
        }
    }
}

/// Every part of both targets finds nothing and can vouch for every key,
/// at every page size.
async fn check(d: &Driver, when: &str) {
    for target in ["l", "i"] {
        for limit in [1, 3, 100] {
            assert_eq!(
                compare(d, target, limit).await,
                (vec![], vec![]),
                "{target}, limit {limit}, {when}"
            );
        }
    }
}

/// Changes neither side's drain has applied, on each side alone and on both
/// sides of the same join keys (a key gaining its first match, one losing
/// its last, a row moving between keys and groups, `NULL` keys), against a
/// correct target: every part (`T` and the visible rows, `P`, `M`, `J`)
/// compares against its tables minus them, so finds nothing, on both kinds,
/// at every page size. Applying them changes nothing either.
#[tokio::test]
async fn ring_rows_not_applied_yet_are_subtracted_on_every_part() {
    let mut d = start(&[LEFT_PAIR, INNER]).await;
    seed(&mut d).await;
    let clean = (vec![], vec![]);
    // The `FROM` side alone.
    exec(
        &d,
        "insert into public.a values (10, 5, 1, 1, 4), (11, 6, 7, 7, 1); \
         update public.a set x = x + 1, ga = 2 where id = 1; \
         update public.a set k = null where id = 2",
    )
    .await;
    let sealed = d.seal().await;
    check(&d, "the FROM side pending").await;
    // The joined side alone.
    exec(
        &d,
        "insert into public.b values (10, 1, 9, 60, 2); \
         update public.b set y = 7 where id = 1; \
         delete from public.b where id = 7",
    )
    .await;
    check(&d, "both sides pending").await;
    d.drain(sealed, "late").await;
    check(&d, "the joined side pending").await;
    // Both sides of the same keys, in one transaction and across two: key
    // (7, 7) gains its first match, key (1, 2) loses its last, a joined row
    // moves to key (1, 1).
    exec(
        &d,
        "begin; \
         insert into public.b values (11, 7, 7, 70, 5); \
         update public.a set x = 3 where id = 11; \
         commit; \
         delete from public.b where id = 3; \
         update public.b set k = 1, gb = 20 where id = 10; \
         insert into public.a values (12, 1, 1, 2, 8)",
    )
    .await;
    check(&d, "both sides of the same keys pending").await;
    d.settle().await;
    check(&d, "settled").await;
    for target in ["l", "i"] {
        assert_eq!(walk(&d, target, 3).await.0, [], "{target}");
    }
    assert_eq!(compare(&d, "l", 2).await, clean);
}

/// A planted error in `T`, and one in `P`, on a group and a cell the ring
/// holds changes for, are reported with their exact deltas in one pass: the
/// changes not applied yet are subtracted, so what remains is the plant.
#[tokio::test]
async fn a_planted_error_with_pending_changes_is_reported_exactly() {
    let mut d = start(&[INNER]).await;
    seed(&mut d).await;
    let (t, t_name) = hidden(&d, "i", "t").await;
    let (p, p_name) = hidden(&d, "i", "p").await;
    // Group 1 (a's rows 1 and 2, key 1, each matching b's rows 1, 2 and
    // 5): 6 pairs, `a.x` summed over them 30.
    exec(
        &d,
        &format!("update {t} set __sum_1 = __sum_1 + 1000000 where ga = 1"),
    )
    .await;
    exec(
        &d,
        &format!("update {p} set __n = __n + 1000 where ga = 1 and k = 1"),
    )
    .await;
    exec(
        &d,
        "update public.a set x = 20 where id = 1; \
         insert into public.b values (12, 5, 1, 1, 1)",
    )
    .await;
    let (found, uncertain) = compare(&d, "i", 100).await;
    assert_eq!(uncertain, Vec::<String>::new());
    assert_eq!(
        found,
        [
            cell(
                "1",
                &format!("{t_name}.__sum_1"),
                Some("1000030"),
                Some("30")
            ),
            cell(
                "1\u{1f}1",
                &format!("{p_name}.__n"),
                Some("1002"),
                Some("2")
            ),
        ]
    );
}

/// A change with no image (a `Recompute`) can't be subtracted: on its own
/// side's part only the cells it may touch are uncertain, and every group of
/// `T`, since any pair may hold its row.
#[tokio::test]
async fn an_image_less_pending_change_leaves_its_cells_and_t_uncertain() {
    let mut d = start(&[INNER]).await;
    seed(&mut d).await;
    d.stage_recomputes("public.a", &["1"]).await;
    // a's row 1: group 1, key 1.
    assert_eq!(
        compare(&d, "i", 100).await,
        (vec![], vec!["p:1\u{1f}1".to_string(), "t:*".to_string()])
    );
    d.stage_recomputes("public.b", &["2"]).await;
    // b's row 2: no group of its own in `i`, key 1.
    assert_eq!(
        compare(&d, "i", 100).await,
        (
            vec![],
            vec![
                "j:1".to_string(),
                "m:1".to_string(),
                "p:1\u{1f}1".to_string(),
                "t:*".to_string()
            ]
        )
    );
}

/// A change from before a `TRUNCATE` of either table that reaches no page
/// until after the truncate's page raised that side's truncate floor will be
/// refused by it, so no part counts it: the comparison doesn't subtract it
/// either, on any part. The driver drains the two segments out of order,
/// which the barrier never does; the floor is what refuses it.
#[tokio::test]
async fn a_pending_change_at_or_below_a_sides_truncate_floor_is_not_subtracted() {
    for table in ["a", "b"] {
        let mut d = start(&[LEFT_PAIR, INNER]).await;
        seed(&mut d).await;
        exec(
            &d,
            "insert into public.a values (10, 1, 1, 1, 4); \
             insert into public.b values (10, 1, 1, 10, 3); \
             update public.a set x = 9 where id = 1; \
             update public.b set y = 5 where id = 1",
        )
        .await;
        let before = d.seal().await;
        exec(
            &d,
            &format!(
                "truncate public.{table}; \
                 insert into public.{table} values (20, 1, 1, 1, 1)"
            ),
        )
        .await;
        let truncate = d.seal().await;
        d.drain(truncate, "a").await;
        check(
            &d,
            &format!("{table} truncated, the change before it pending"),
        )
        .await;
        d.drain(before, "b").await;
        check(
            &d,
            &format!("{table} truncated, the change before it refused"),
        )
        .await;
        for target in ["l", "i"] {
            assert_eq!(walk(&d, target, 3).await.0, [], "{table}: {target}");
        }
    }
}

/// A cell a recompute rewrote from a snapshot that saw a change the drain
/// hasn't applied yet already counts that change, and its page will refuse
/// it, on `P` or `M` and so on `J` and `T`: the comparison doesn't subtract
/// it either, on any part. The driver drains the recompute's segment before
/// the change's, which the barrier never does; the cell's basis is what
/// refuses it.
#[tokio::test]
async fn a_pending_change_a_cells_basis_saw_is_not_subtracted() {
    for table in ["a", "b"] {
        let mut d = start(&[LEFT_PAIR, INNER]).await;
        seed(&mut d).await;
        let column = if table == "a" { "x" } else { "y" };
        exec(
            &d,
            &format!("update public.{table} set {column} = 50, k = 2 where id = 1"),
        )
        .await;
        let change = d.seal().await;
        d.stage_recomputes(&format!("public.{table}"), &["1"]).await;
        let recompute = d.seal().await;
        d.drain(recompute, "a").await;
        check(&d, &format!("{table}: the basis saw the change")).await;
        d.drain(change, "b").await;
        check(&d, &format!("{table}: the change refused")).await;
        for target in ["l", "i"] {
            assert_eq!(walk(&d, target, 3).await.0, [], "{table}: {target}");
        }
    }
}

/// A `FROM`-side group column re-collated (between deterministic
/// collations, which pauses nothing) while a walk pages through `P`: the
/// part pages under the collation its hidden table was made with, not the
/// source's new one, so the walk still reads every cell once (#1178).
#[tokio::test]
async fn a_group_column_re_collated_mid_walk_skips_and_repeats_no_cell() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start_with_database_defaults(
        &[],
        "create table public.a (id integer primary key, ga text collate \"C\", k integer); \
         create table public.b (id integer primary key, k integer, y integer); \
         create index on public.a (k); create index on public.b (k)",
        &[
            ("id", int4()),
            ("ga", ValueType::Text),
            ("k", int4()),
            ("y", int4()),
        ],
        &[],
        &[
            "TRANSFORM tc FROM public.a JOIN public.b ON a.k = b.k GROUP BY a.ga \
           SELECT a.ga AS ga, COUNT(*) AS n, SUM(b.y) AS sy",
        ],
        &["public.a", "public.b"],
    )
    .await;
    // Byte order puts the capitals first; `und-x-icu` interleaves them.
    step(
        &mut d,
        "insert into public.a values (1, 'a', 1), (2, 'B', 1), (3, 'c', 2), (4, 'D', 2), \
                                     (5, 'e', 1), (6, 'F', 2); \
         insert into public.b values (1, 1, 10), (2, 2, 20)",
    )
    .await;
    let (found, whole, _) = walk(&d, "tc", 1).await;
    assert_eq!(found, []);

    let (mut after, mut compared, mut altered) = (None, 0, false);
    loop {
        let page = check_page(
            d.pool(),
            "tc",
            SelfCheckScope { after, limit: 1 },
            SelfCheckMode::Standard,
            Duration::from_secs(30),
        )
        .await
        .expect("a page of the check");
        compared += page.rows_compared;
        assert!(
            matches!(page.outcome, SelfCheckOutcome::Converged),
            "{:?}",
            page.outcome
        );
        after = page.next_after;
        if !altered && after.as_deref().is_some_and(|a| a.starts_with("p:")) {
            exec(
                &d,
                "alter table public.a alter column ga type text collate \"und-x-icu\"",
            )
            .await;
            altered = true;
        }
        if after.is_none() {
            break;
        }
    }
    assert!(altered, "the walk paged through P");
    assert_eq!(
        compared, whole,
        "every key once, as a walk without the change"
    );
}

/// Group columns that are also the join key, and a joined side with no group
/// columns.
const GROUP_IS_KEY: &str = "TRANSFORM gk FROM public.a JOIN public.b ON a.k = b.k \
     GROUP BY a.k, b.gb SELECT a.k AS k, b.gb AS gb, COUNT(*) AS n, SUM(a.x) AS sx";

/// A `P` or `M` page orders by the hidden cell identity, the join key first
/// (#1198): the hidden side is a range scan of the hidden table's one index
/// and the source side a range scan of an index on the source's join columns.
#[tokio::test]
async fn a_p_or_m_page_is_a_range_scan_of_the_hidden_index_and_the_source_join_index() {
    let d = start(&[LEFT_PAIR]).await;
    exec(
        &d,
        "create index a_join_idx on public.a (t, k); create index b_join_idx on public.b (t, k)",
    )
    .await;
    for (part, source, index, group) in [
        ("p", "a", "a_join_idx", "ga"),
        ("m", "b", "b_join_idx", "gb"),
    ] {
        let (hidden, name) = hidden(&d, "l", part).await;
        // The hidden table's one index, whatever define named it.
        let hidden_index = d
            .rows(&format!(
                "select c.relname from pg_index i join pg_class c on c.oid = i.indexrelid \
                 where i.indrelid = '{hidden}'::regclass"
            ))
            .await;
        let [hidden_index] = hidden_index.as_slice() else {
            panic!("{part}: one index on {hidden}: {hidden_index:?}");
        };
        let hidden_index = hidden_index.trim_matches(|c| c == '(' || c == ')');
        // Statistics, so the plan does not depend on whether autovacuum has
        // analyzed the tables yet.
        exec(
            &d,
            &format!("analyze public.a; analyze public.b; analyze {hidden}"),
        )
        .await;
        let plan = trellis::staging::self_check::explain_join_page(
            d.pool(),
            "public.l",
            part,
            Some("1\u{1f}1\u{1f}1"),
            3,
        )
        .await
        .expect("explain the page");
        // A range scan of `index` on `table` (plain, index-only or the bitmap
        // index scan under a bitmap heap scan), and its lines. A seq scan is
        // none of these.
        let scan_of = |index: &str, table: &str, cond: &str| -> Option<String> {
            let lines: Vec<&str> = plan.lines().collect();
            let at = (0..lines.len()).find(|&i| {
                let l = &lines[i];
                let block = lines[i..(i + 3).min(lines.len())].join("\n");
                block.contains(cond)
                    && (l.contains(&format!("Index Scan using {index} on {table} "))
                        || l.contains(&format!("Index Only Scan using {index} on {table} "))
                        || l.contains(&format!("Bitmap Index Scan on {index} ")))
            })?;
            Some(lines[at..(at + 3).min(lines.len())].join("\n"))
        };
        // The hidden side's range is bounded by the cursor in the index's
        // own column order: the join key, then the group column.
        let scan = scan_of(
            hidden_index,
            &name,
            &format!("Index Cond: (ROW(t, k, {group}) > "),
        );
        assert!(
            scan.is_some(),
            "{part}: the hidden side is a range scan of {hidden}'s index:\n{plan}"
        );
        assert!(
            !plan.contains(&format!("Seq Scan on {name} __h")),
            "{part}: the hidden table is not scanned whole:\n{plan}"
        );
        // The source side's range is bounded by the join key alone, so an
        // index on the join columns serves it.
        assert!(
            scan_of(index, source, "(ROW(t, k) >= ").is_some(),
            "{part}: the source side is a range scan of {index}:\n{plan}"
        );
        // The page steers by that index, the one define requires, though the
        // page's key goes on past the join columns to the group column
        // (#1223): its groups come out of the index in order, so the source
        // side is not bounded by the hidden side's last key (`he`).
        assert!(
            !plan.contains("CTE Scan on he"),
            "{part}: the page steers by {index}:\n{plan}"
        );
    }
}

/// A walk's findings and the keys it compares do not depend on the page
/// size, whatever the key's shape: the walk reads every cell once. Every
/// cell of `P` and `M` is damaged, so a skipped or repeated key shows as a
/// missing or doubled divergence.
async fn walk_is_the_same_at_every_page_size(definition: &str, target: &str) {
    let mut d = start(&[definition]).await;
    seed(&mut d).await;
    for part in ["p", "m"] {
        let (table, _) = hidden(&d, target, part).await;
        exec(&d, &format!("update {table} set __n = __n + 1")).await;
    }
    let sorted = |found: Vec<Divergence>| {
        let mut found: Vec<String> = found.iter().map(|f| format!("{f:?}")).collect();
        found.sort();
        found
    };
    let (whole, compared, _) = walk(&d, target, 1000).await;
    let whole = sorted(whole);
    assert!(whole.len() > 6, "{whole:?}");
    for limit in [1, 2, 3, 5] {
        let (found, count, _) = walk(&d, target, limit).await;
        let found = sorted(found);
        assert_eq!(found, whole, "limit {limit}");
        assert_eq!(count, compared, "limit {limit}");
    }
}

#[tokio::test]
async fn a_composite_nullable_join_key_walks_alike_at_every_page_size() {
    walk_is_the_same_at_every_page_size(LEFT_PAIR, "l").await;
}

#[tokio::test]
async fn a_joined_side_with_no_group_columns_walks_alike_at_every_page_size() {
    walk_is_the_same_at_every_page_size(INNER, "i").await;
}

#[tokio::test]
async fn a_group_column_that_is_also_the_join_key_walks_alike_at_every_page_size() {
    walk_is_the_same_at_every_page_size(GROUP_IS_KEY, "gk").await;
}

/// Cells planted on one side alone of `P` and `M`, at every other key in the
/// walk's order, so each lands on a page boundary at some page size: every
/// one is found at every page size, as the unpaged walk finds it. A source
/// key past the hidden side's last key of a page belongs to the next page,
/// so a source and hidden side ordered or bounded differently would skip or
/// double one. `parts` names each part's hidden key columns in the walk's
/// order (the join key first) and the group column whose value an extra cell
/// varies, if the side has one apart from its join key.
async fn one_sided_cells_are_found_at_every_page_size(
    definition: &str,
    target: &str,
    parts: [(&str, &str, Option<&str>); 2],
) {
    let mut d = start(&[definition]).await;
    seed(&mut d).await;
    let mut planted = 0;
    for (part, key, group) in parts {
        let (table, _) = hidden(&d, target, part).await;
        // Every other live cell in the walk's order (each column ascending,
        // NULL last): gone from the hidden side alone, a cell only the
        // source has. (An emptied cell, `__n = 0`, matches its absence.)
        let live = format!(
            "select ctid from (select ctid, row_number() over (order by {key}) as rn \
             from {table} where __n <> 0) r where rn % 2 = 0"
        );
        let deleted = d.rows(&live).await.len();
        assert!(deleted >= 1, "{part}: a cell to delete");
        exec(&d, &format!("delete from {table} where ctid in ({live})")).await;
        planted += deleted;
        // Cells only the hidden side has, interleaved with the real ones:
        // each remaining join key under a group value no row has.
        if let Some(group) = group {
            let join: Vec<&str> = key.split(", ").filter(|c| *c != group).collect();
            let join = join.join(", ");
            exec(
                &d,
                &format!(
                    "insert into {table} ({join}, {group}, __n) \
                     select distinct {join}, 1000, 5 from {table}"
                ),
            )
            .await;
            planted += d
                .rows(&format!("select 1 from {table} where __n = 5"))
                .await
                .len();
        }
    }
    let sorted = |found: &[Divergence]| -> Vec<String> {
        let mut found: Vec<String> = found.iter().map(|f| format!("{f:?}")).collect();
        found.sort();
        found
    };
    let (whole, compared, _) = walk(&d, target, 1000).await;
    let whole = sorted(&whole);
    let keys: std::collections::BTreeSet<String> = whole
        .iter()
        .filter(|f| f.contains("_p.") || f.contains("_m."))
        .map(|f| {
            let (key, rest) = f.split_once(", column").unwrap();
            format!("{key} {}", &rest[..rest.find('.').unwrap()])
        })
        .collect();
    assert_eq!(
        keys.len(),
        planted,
        "every planted cell is found unpaged, once: {whole:#?}"
    );
    for limit in [1, 2, 3, 4, 5] {
        let (found, count, _) = walk(&d, target, limit).await;
        assert_eq!(sorted(&found), whole, "limit {limit}");
        assert_eq!(count, compared, "limit {limit}");
    }
    // Again with each source's index on its join columns, which the page
    // then steers by instead of bounding the source by the hidden side.
    for (part, key, group) in parts {
        let source = if part == "p" { "a" } else { "b" };
        let join = key
            .split(", ")
            .filter(|c| Some(*c) != group)
            .collect::<Vec<_>>();
        exec(
            &d,
            &format!("create index on public.{source} ({})", join.join(", ")),
        )
        .await;
    }
    for limit in [1, 2, 3, 4, 5] {
        let (found, count, _) = walk(&d, target, limit).await;
        assert_eq!(sorted(&found), whole, "indexed, limit {limit}");
        assert_eq!(count, compared, "indexed, limit {limit}");
    }
}

#[tokio::test]
async fn one_sided_cells_of_a_composite_nullable_join_key_are_found_at_every_page_size() {
    one_sided_cells_are_found_at_every_page_size(
        LEFT_PAIR,
        "l",
        [("p", "t, k, ga", Some("ga")), ("m", "t, k, gb", Some("gb"))],
    )
    .await;
}

#[tokio::test]
async fn one_sided_cells_of_a_joined_side_with_no_group_columns_are_found_at_every_page_size() {
    one_sided_cells_are_found_at_every_page_size(
        INNER,
        "i",
        [("p", "k, ga", Some("ga")), ("m", "k", None)],
    )
    .await;
}

#[tokio::test]
async fn one_sided_cells_where_a_group_column_is_the_join_key_are_found_at_every_page_size() {
    one_sided_cells_are_found_at_every_page_size(
        GROUP_IS_KEY,
        "gk",
        [("p", "k", None), ("m", "k, gb", Some("gb"))],
    )
    .await;
}

/// Every `JOIN` part's page skips the drained ring slots (#1219): with
/// the pipeline drained, no ring slot holding a drained segment is scanned,
/// whichever source's ring rows the part subtracts, and a comparison still
/// finds nothing.
#[tokio::test]
async fn a_join_page_never_scans_a_drained_ring_slot() {
    let mut d = start(&[LEFT_PAIR]).await;
    seed(&mut d).await;
    step(&mut d, "insert into public.a values (20, 1, 1, 1, 1)").await;
    step(&mut d, "insert into public.b values (20, 1, 1, 1, 1)").await;
    for part in ["p", "m", "j", "t"] {
        let plan = trellis::staging::self_check::explain_join_page_executed(
            d.pool(),
            "public.l",
            part,
            None,
            5,
        )
        .await
        .expect("explain the page as run");
        let scans: Vec<&str> = plan
            .lines()
            .filter(|l| l.contains("Scan on seg_"))
            .collect();
        assert!(!scans.is_empty(), "{part}: {plan}");
        // The segment before the active one may be read (its successor's
        // fence can have had a writer in flight); every older slot and every
        // unused slot is skipped.
        let skipped = scans
            .iter()
            .filter(|l| l.contains("(never executed)"))
            .count();
        assert!(
            skipped * 2 >= scans.len(),
            "{part}: {skipped} of {} ring scans skipped:\n{plan}",
            scans.len()
        );
    }
    assert_eq!(walk(&d, "public.l", 3).await.0, []);
}

/// `Trellis::request_backfill` of `table`, then the rebuild's sweep job run
/// by hand to its commit (as `join_interleavings.rs`): the rebuild in place
/// a user asks for.
async fn request_backfill_and_rebuild(d: &Driver, target: &str, table: &str) {
    let trellis = Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    trellis
        .request_backfill(table)
        .await
        .expect("request a rebuild");
    assert_eq!(
        d.rows("select kind from backfill_chunks").await,
        ["(sweep)"],
        "a JOIN rebuild is its sweep job alone"
    );
    trellis::staging::build::settle_builds(d.pool()).await;
    assert_eq!(
        d.rows(&format!(
            "select status, (select count(*) from backfill_chunks) \
             from transform_definitions where target_table = 'public.{target}'"
        ))
        .await,
        ["(live,0)"],
        "the rebuild committed and left the definition live"
    );
}

/// The hidden tables `tables` each have a divergence in `found`.
fn assert_reported_on(found: &[Divergence], tables: &[&str]) {
    for table in tables {
        assert!(
            found
                .iter()
                .any(|d| matches!(d, Divergence::Cell { column, .. }
                if column.starts_with(&format!("{table}.")))),
            "{table}: {found:?}"
        );
    }
}

/// #1112's acceptance for `P` and `M`: a write made on each side while
/// capture was off leaves `P`, `M` (and `J`, `T` and the target, which follow
/// them by delta) stale; the check reports it, `request_backfill` of each
/// side (a rebuild in place, which writes `P`/`M` absolutely and pushes the
/// difference on to `J` and `T`) repairs all of them, and the check is then
/// clean.
///
/// Only a write capture missed is planted: a hand edit of `P` alone would be
/// pushed through to `T` by the rebuild as well (#1308).
async fn p_and_m_are_repaired_by_request_backfill(definition: &str, target: &str) {
    let mut d = start(&[definition]).await;
    seed(&mut d).await;
    assert_eq!(
        walk(&d, target, 100).await.0,
        [],
        "{target}: before the plant"
    );
    let (_, p) = hidden(&d, target, "p").await;
    let (_, m) = hidden(&d, target, "m").await;

    for table in ["a", "b"] {
        exec(
            &d,
            &format!("alter table public.{table} disable trigger trellis_capture_insert"),
        )
        .await;
    }
    exec(&d, "insert into public.a values (20, 6, 1, 3, 1)").await;
    exec(&d, "insert into public.b values (20, 1, 3, 5, 1)").await;
    for table in ["a", "b"] {
        exec(
            &d,
            &format!("alter table public.{table} enable always trigger trellis_capture_insert"),
        )
        .await;
    }
    d.settle().await;

    let (_, j) = hidden(&d, target, "j").await;
    let (_, t) = hidden(&d, target, "t").await;
    for limit in [1, 100] {
        let found = walk(&d, target, limit).await.0;
        assert_reported_on(&found, &[&p, &m, &j, &t]);
        assert!(
            found
                .iter()
                .any(|d| matches!(d, Divergence::MissingRow { .. })),
            "{target}: the target lacks the new group: {found:?}"
        );
    }

    request_backfill_and_rebuild(&d, target, "a").await;
    request_backfill_and_rebuild(&d, target, "b").await;

    let (found, compared, _) = walk(&d, target, 100).await;
    assert_eq!(found, [], "{target}: the rebuilds repaired every plant");
    assert!(compared > 0);
    assert_eq!(walk(&d, target, 1).await.0, []);
}

#[tokio::test]
async fn a_capture_off_write_is_reported_until_request_backfill_repairs_it_left_pair() {
    p_and_m_are_repaired_by_request_backfill(LEFT_PAIR, "l").await;
}

#[tokio::test]
async fn a_capture_off_write_is_reported_until_request_backfill_repairs_it_inner() {
    p_and_m_are_repaired_by_request_backfill(INNER, "i").await;
}

/// The documented limit (known-correctness-gaps: "Rows written into a target,
/// a hidden table or Trellis's schema"): `request_backfill` does not repair
/// a hand edit of `join_<id>_t` or `join_<id>_j`, because they change only
/// by delta (#1097); `PAUSE`/`RESUME` recreates the hidden tables and does.
#[tokio::test]
async fn j_and_t_divergences_are_reported_until_resume_repairs_them() {
    let mut d = start(&[INNER]).await;
    seed(&mut d).await;
    let (j, j_name) = hidden(&d, "i", "j").await;
    let (t, t_name) = hidden(&d, "i", "t").await;
    exec(
        &d,
        &format!("update {j} set __matches = __matches + 1 where k = 1"),
    )
    .await;
    exec(&d, &format!("update {t} set __n = __n + 5 where ga = 1")).await;
    let planted = [
        cell("1", &format!("{t_name}.__n"), Some("11"), Some("6")),
        cell("1", &format!("{j_name}.__matches"), Some("4"), Some("3")),
    ];
    assert_eq!(walk(&d, "i", 100).await.0, planted);

    // A rebuild in place of either side leaves both edits.
    request_backfill_and_rebuild(&d, "i", "a").await;
    request_backfill_and_rebuild(&d, "i", "b").await;
    // The rebuild renders the visible row from the edited `T`, so the
    // target now carries the edit too.
    assert_eq!(
        walk(&d, "i", 100).await.0,
        [
            planted[0].clone(),
            cell("1", "n", Some("11"), Some("6")),
            planted[1].clone()
        ],
        "request_backfill doesn't repair J or T"
    );

    // Pause the definition and resume it: the plan job rebuilds the hidden
    // tables from the sources.
    exec(
        &d,
        "update transform_definitions set status = 'paused' where target_table = 'public.i'",
    )
    .await;
    trellis::staging::quarantine::resume_transform(d.pool(), "i")
        .await
        .expect("resume");
    trellis::intake::markers::discharge_registrations(d.pool())
        .await
        .expect("start the build");
    assert_eq!(
        d.rows("select kind from backfill_chunks").await,
        ["(plan)"],
        "a resume is a fresh build"
    );
    trellis::staging::build::settle_builds(d.pool()).await;
    assert_eq!(
        d.rows("select status from transform_definitions where target_table = 'public.i'")
            .await,
        ["(live)"]
    );
    let (found, compared, _) = walk(&d, "i", 100).await;
    assert_eq!(found, [], "the resume recreated J and T");
    assert!(compared > 0);
}
