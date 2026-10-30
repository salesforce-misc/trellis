//! `self_check` of a 1-1 `JOIN` target (ADR-0013 "Scope", ADR-0021, #1402).
//! It detects and never repairs: the target is compared against the live join,
//! `R` and `M` against their sources, and a key with more than one live `M`
//! entry is reported. Each check has a planted divergence that is reported on
//! that structure alone (the exact divergences are asserted) and gone once the
//! planted damage is undone by hand, for `LEFT` and `INNER`.
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`
//! and never polls for convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::integer::IntWidth;
use trellis::staging::self_check::{
    CheckPageExt, Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page,
    compare_page,
};
use trellis::{Config, Trellis, TrellisOptions};

const DDL: &str = "create table public.a (id text primary key, k integer, r integer, s text); \
                   create table public.b (bid bigint primary key, k integer, img integer, \
                       k0 text); \
                   create unique index b_k_unique on public.b (k); \
                   create index on public.a (k)";

/// Twenty `a` rows on keys 0 to 60 (every fifth on `NULL`), six `b` rows on
/// keys 10 to 60: some `a` rows have no match. Fourteen have one.
const SEED: &str = "insert into public.a \
                        select 'a' || lpad(i::text, 2, '0'), \
                               case when i % 5 = 0 then null else (i % 7) * 10 end, \
                               i, 's' || i \
                        from generate_series(1, 20) i; \
                    insert into public.b select i, i * 10, i * 100, 'k' || i \
                        from generate_series(1, 6) i";

/// The dev knob is process-wide.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Left,
    Inner,
}

const KINDS: [Kind; 2] = [Kind::Left, Kind::Inner];

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Left => "jl",
            Kind::Inner => "ji",
        }
    }

    fn target(self) -> String {
        format!("public.{}", self.name())
    }

    fn word(self) -> &'static str {
        match self {
            Kind::Left => "LEFT",
            Kind::Inner => "INNER",
        }
    }

    fn definition(self) -> String {
        format!(
            "TRANSFORM {} FROM public.a {} JOIN public.b ON a.k = b.k \
             SELECT a.r + COALESCE(b.img, 0) AS total, b.k0 AS k0, a.s AS s",
            self.name(),
            self.word()
        )
    }
}

fn columns() -> Vec<(&'static str, ValueType)> {
    vec![
        ("id", ValueType::Text),
        ("k", ValueType::Integer(IntWidth::Int4)),
        ("r", ValueType::Integer(IntWidth::Int4)),
        ("s", ValueType::Text),
    ]
}

async fn start(built: bool) -> Driver {
    set_enabled(true);
    let ddl = format!("{DDL}; {SEED}");
    let definitions: Vec<String> = KINDS.iter().map(|k| k.definition()).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let columns = columns();
    if built {
        Driver::start(&ddl, &columns, &definitions, &["public.a", "public.b"]).await
    } else {
        Driver::start_unbuilt(&[], &ddl, &columns, &definitions, &["public.a", "public.b"]).await
    }
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it as one sealed batch.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

async fn definition_id(d: &Driver, kind: Kind) -> i64 {
    d.ctl
        .query_one(
            "select id from transform_definitions where target_table = $1",
            &[&kind.target()],
        )
        .await
        .expect("the definition")
        .get(0)
}

/// `R`'s and `M`'s qualified names for `kind`, and the names a divergence
/// gives their columns.
struct Hidden {
    r: String,
    m: String,
    r_name: String,
    m_name: String,
}

async fn hidden(d: &Driver, kind: Kind) -> Hidden {
    let id = definition_id(d, kind).await;
    Hidden {
        r: format!("{DEFAULT_SCHEMA}.join1_{id}_r"),
        m: format!("{DEFAULT_SCHEMA}.join1_{id}_m"),
        r_name: format!("join1_{id}_r"),
        m_name: format!("join1_{id}_m"),
    }
}

/// What a walk of the check found.
#[derive(Debug)]
struct Walked {
    divergences: Vec<Divergence>,
    compared: i64,
    pages: usize,
}

/// Every page of the check of `target`, `limit` keys at a time.
async fn walk(d: &Driver, target: &str, limit: i64) -> Walked {
    let mut after = None;
    let mut walked = Walked {
        divergences: Vec::new(),
        compared: 0,
        pages: 0,
    };
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
        walked.pages += 1;
        walked.compared += page.rows_compared;
        match page.outcome {
            SelfCheckOutcome::Converged => {}
            SelfCheckOutcome::Diverged(mut found) => walked.divergences.append(&mut found),
            other => panic!("unexpected outcome {other:?}"),
        }
        after = page.next_after;
        if after.is_none() {
            walked.divergences.sort_by_key(|d| format!("{d:?}"));
            return walked;
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

fn sorted(mut found: Vec<Divergence>) -> Vec<Divergence> {
    found.sort_by_key(|d| format!("{d:?}"));
    found
}

/// Asserts `kind`'s check finds exactly `expected` at every page size.
async fn assert_found(d: &Driver, kind: Kind, expected: Vec<Divergence>, when: &str) {
    let expected = sorted(expected);
    for limit in [2, 1000] {
        let walked = walk(d, &kind.target(), limit).await;
        assert_eq!(
            walked.divergences, expected,
            "{kind:?} at {limit} keys a page, {when}"
        );
    }
}

/// The check of every kind finds nothing at every page size.
async fn assert_clean(d: &Driver, when: &str) {
    for kind in KINDS {
        assert_found(d, kind, Vec::new(), when).await;
    }
}

// --------------------------------------------------------------- a clean walk

/// Keys compared by a clean walk: the target's (the `a` rows, or the matched
/// ones), `R`'s (`a`'s rows, and again for the entries looked up), `M`'s (`b`'s
/// rows twice), and `M`'s entries once more, by join key, for duplicates.
fn clean_keys(kind: Kind) -> i64 {
    match kind {
        Kind::Left => 20 + 20 + 20 + 6 + 6 + 6,
        Kind::Inner => 14 + 20 + 20 + 6 + 6 + 6,
    }
}

#[tokio::test]
async fn a_converged_target_reports_no_divergence_at_every_page_size() {
    let _serial = SERIAL.lock().await;
    let mut d = start(true).await;
    // Key moves, a row that loses its match and one that gains it, deletes and
    // an insert on each side, and a `b` key handed to another row.
    for sql in [
        "update public.a set k = 30 where id = 'a01'",
        "update public.a set k = null where id = 'a02'",
        "update public.a set k = 40 where id = 'a05'",
        "delete from public.a where id = 'a03'",
        "insert into public.a values ('a21', 50, 21, 's21'), ('a22', 99, 22, 's22')",
        "update public.b set img = 999 where bid = 2",
        "delete from public.b where bid = 4",
        "insert into public.b values (7, 99, 700, 'k7')",
        "update public.b set k = 70 where bid = 6",
        "update public.b set k = 60 where bid = 6",
    ] {
        step(&mut d, sql).await;
    }
    for kind in KINDS {
        let first = walk(&d, &kind.target(), 1000).await;
        assert_eq!(first.divergences, Vec::new(), "{kind:?}");
        let sizes: Vec<(i64, usize)> = {
            let mut sizes = Vec::new();
            for limit in [1, 2, 7, 1000] {
                let walked = walk(&d, &kind.target(), limit).await;
                assert_eq!(walked.divergences, Vec::new(), "{kind:?} at {limit}");
                assert_eq!(walked.compared, first.compared, "{kind:?} at {limit}");
                sizes.push((limit, walked.pages));
            }
            sizes
        };
        // A smaller page takes more pages to walk the same keys.
        assert!(
            sizes[0].1 > sizes[1].1 && sizes[1].1 > sizes[3].1,
            "{sizes:?}"
        );
    }
}

#[tokio::test]
async fn a_clean_walk_compares_every_key_of_every_part_once() {
    let _serial = SERIAL.lock().await;
    let d = start(true).await;
    for kind in KINDS {
        let walked = walk(&d, &kind.target(), 1000).await;
        assert_eq!(walked.divergences, Vec::new());
        assert_eq!(walked.compared, clean_keys(kind), "{kind:?}");
        // One page for each of the six parts, and the walk ends with the last.
        assert_eq!(walked.pages, 6, "{kind:?}");
        // A page of the duplicates part reads its limit of `M`'s entries
        // whether or not a key is duplicated: six entries take three full
        // pages of two, then one that finds the part's end.
        let mut after = Some("d".to_string());
        let mut pages = 0;
        while let Some(cursor) = after.filter(|c| c.starts_with('d')) {
            let page = check_page(
                d.pool(),
                &kind.target(),
                SelfCheckScope {
                    after: Some(cursor),
                    limit: 2,
                },
                SelfCheckMode::Standard,
                Duration::from_secs(30),
            )
            .await
            .expect("a page of the duplicates part");
            assert_eq!(page.outcome, SelfCheckOutcome::Converged, "{kind:?}");
            pages += 1;
            after = page.next_after;
        }
        assert_eq!(pages, 4, "{kind:?}");
    }
}

// ------------------------------------------------------ planted divergences

/// Plants `plant` in `kind`, finds exactly `expected` at every page size, and
/// after `undo` finds nothing.
async fn planted(d: &Driver, kind: Kind, plant: &str, undo: &str, expected: Vec<Divergence>) {
    write(d, plant).await;
    assert_found(d, kind, expected, plant).await;
    write(d, undo).await;
    assert_found(d, kind, Vec::new(), undo).await;
}

#[tokio::test]
async fn a_planted_divergence_in_the_target_is_reported_until_it_is_undone() {
    let _serial = SERIAL.lock().await;
    let d = start(true).await;
    for kind in KINDS {
        let t = kind.name();
        // a02 has the match 2: 2 + 200.
        planted(
            &d,
            kind,
            &format!("update public.{t} set total = total + 1 where id = 'a02'"),
            &format!("update public.{t} set total = total - 1 where id = 'a02'"),
            vec![cell("a02", "total", Some("203"), Some("202"))],
        )
        .await;
        planted(
            &d,
            kind,
            &format!("update public.{t} set k0 = null where id = 'a02'"),
            &format!("update public.{t} set k0 = 'k2' where id = 'a02'"),
            vec![cell("a02", "k0", None, Some("k2"))],
        )
        .await;
        planted(
            &d,
            kind,
            &format!("delete from public.{t} where id = 'a03'"),
            &format!("insert into public.{t} values ('a03', 303, 'k3', 's3')"),
            vec![Divergence::MissingRow {
                key: "a03".to_string(),
            }],
        )
        .await;
        planted(
            &d,
            kind,
            &format!("insert into public.{t} values ('zz', 1, 'x', 'y')"),
            &format!("delete from public.{t} where id = 'zz'"),
            vec![Divergence::ExtraRow {
                key: "zz".to_string(),
            }],
        )
        .await;
        // a07 is on key 0, which `b` has no row for: a `LEFT` row without its
        // `b` columns, no row at all for `INNER`.
        match kind {
            Kind::Left => {
                planted(
                    &d,
                    kind,
                    &format!("update public.{t} set k0 = 'k1' where id = 'a07'"),
                    &format!("update public.{t} set k0 = null where id = 'a07'"),
                    vec![cell("a07", "k0", Some("k1"), None)],
                )
                .await;
            }
            Kind::Inner => {
                planted(
                    &d,
                    kind,
                    &format!("insert into public.{t} values ('a07', 7, 'k1', 's7')"),
                    &format!("delete from public.{t} where id = 'a07'"),
                    vec![Divergence::ExtraRow {
                        key: "a07".to_string(),
                    }],
                )
                .await;
            }
        }
    }
    assert_clean(&d, "after every plant is undone").await;
}

#[tokio::test]
async fn a_planted_divergence_in_r_is_reported_until_it_is_undone() {
    let _serial = SERIAL.lock().await;
    let d = start(true).await;
    for kind in KINDS {
        let h = hidden(&d, kind).await;
        let col = |c: &str| format!("{}.{c}", h.r_name);
        // A copied value that differs from `a`.
        planted(
            &d,
            kind,
            &format!("update {} set r = r + 1 where __from_key = 'a02'", h.r),
            &format!("update {} set r = r - 1 where __from_key = 'a02'", h.r),
            vec![cell("a02", &col("r"), Some("3"), Some("2"))],
        )
        .await;
        // A copied join key that differs from `a`'s (and a `NULL` one).
        planted(
            &d,
            kind,
            &format!("update {} set k = null where __from_key = 'a02'", h.r),
            &format!("update {} set k = 20 where __from_key = 'a02'", h.r),
            vec![cell("a02", &col("k"), None, Some("20"))],
        )
        .await;
        // An `a` row with no entry, and one whose entry is a tombstone.
        planted(
            &d,
            kind,
            &format!(
                "update {} set __tombstone = true where __from_key = 'a03'",
                h.r
            ),
            &format!(
                "update {} set __tombstone = false where __from_key = 'a03'",
                h.r
            ),
            vec![cell("a03", &col("__entry"), None, Some("live"))],
        )
        .await;
        // The entry of a row that `a` has not: it is `a`'s row looked up by
        // the entry's key, and found missing.
        planted(
            &d,
            kind,
            &format!(
                "insert into {} (__from_key, k, r, s) values ('zz', 10, 1, 'x')",
                h.r
            ),
            &format!("delete from {} where __from_key = 'zz'", h.r),
            vec![cell("zz", &col("__entry"), Some("live"), None)],
        )
        .await;
        // Deleting the entry of a row, which a later `a` write would see as an
        // insert, leaves the row without one.
        let saved = d
            .rows(&format!(
                "select k, r, s from {} where __from_key = 'a04'",
                h.r
            ))
            .await;
        assert_eq!(saved.len(), 1);
        planted(
            &d,
            kind,
            &format!("delete from {} where __from_key = 'a04'", h.r),
            &format!(
                "insert into {} (__from_key, k, r, s) select 'a04', k, r, s from public.a \
                 where id = 'a04'",
                h.r
            ),
            vec![cell("a04", &col("__entry"), None, Some("live"))],
        )
        .await;
    }
}

#[tokio::test]
async fn a_planted_divergence_in_m_is_reported_until_it_is_undone() {
    let _serial = SERIAL.lock().await;
    let d = start(true).await;
    for kind in KINDS {
        let h = hidden(&d, kind).await;
        let col = |c: &str| format!("{}.{c}", h.m_name);
        planted(
            &d,
            kind,
            &format!("update {} set img = img + 1 where __from_key = '2'", h.m),
            &format!("update {} set img = img - 1 where __from_key = '2'", h.m),
            vec![cell("2", &col("img"), Some("201"), Some("200"))],
        )
        .await;
        planted(
            &d,
            kind,
            &format!("update {} set k0 = 'x' where __from_key = '3'", h.m),
            &format!("update {} set k0 = 'k3' where __from_key = '3'", h.m),
            vec![cell("3", &col("k0"), Some("x"), Some("k3"))],
        )
        .await;
        planted(
            &d,
            kind,
            &format!(
                "update {} set __tombstone = true where __from_key = '5'",
                h.m
            ),
            &format!(
                "update {} set __tombstone = false where __from_key = '5'",
                h.m
            ),
            vec![cell("5", &col("__entry"), None, Some("live"))],
        )
        .await;
        // An entry of a `b` row that is gone, on a key `b` has no other row
        // for.
        planted(
            &d,
            kind,
            &format!(
                "insert into {} (__from_key, k, img, k0) values ('99', 80, 1, 'x')",
                h.m
            ),
            &format!("delete from {} where __from_key = '99'", h.m),
            vec![cell("99", &col("__entry"), Some("live"), None)],
        )
        .await;
    }
}

/// `b` loses its uniqueness (the index dropped, a second row on a key): `M`
/// holds two live entries on the key and the target reads the newest, which
/// masks the duplicate. The check names the key, and nothing else is wrong.
#[tokio::test]
async fn a_key_with_two_live_m_entries_is_reported() {
    let _serial = SERIAL.lock().await;
    let mut d = start(true).await;
    step(&mut d, "drop index b_k_unique").await;
    step(&mut d, "insert into public.b values (7, 30, 777, 'dup')").await;
    for kind in KINDS {
        let h = hidden(&d, kind).await;
        let live: i64 = d
            .ctl
            .query_one(
                &format!(
                    "select count(*) from {} where k = 30 and not __tombstone",
                    h.m
                ),
                &[],
            )
            .await
            .expect("count")
            .get(0);
        assert_eq!(live, 2, "M holds both rows of the key");
        // Only the duplicate: `R`, `M` and the target agree with `a` and `b`
        // (the target reads whichever of the two the newest entry names).
        assert_found(
            &d,
            kind,
            vec![cell(
                "30",
                &format!("{}.live_entries", h.m_name),
                Some("2"),
                Some("1"),
            )],
            "with b's key 30 on two rows",
        )
        .await;
    }
    // The second row goes: the key is unique again.
    step(&mut d, "delete from public.b where bid = 7").await;
    assert_clean(&d, "after the duplicate is deleted").await;
    // Two keys with duplicates are two cells, and a `NULL` key is none.
    step(
        &mut d,
        "insert into public.b values (7, 30, 777, 'dup'), (8, 20, 888, 'dup2'), \
         (9, null, 1, 'n1'), (10, null, 2, 'n2')",
    )
    .await;
    for kind in KINDS {
        let h = hidden(&d, kind).await;
        let col = format!("{}.live_entries", h.m_name);
        assert_found(
            &d,
            kind,
            vec![
                cell("20", &col, Some("2"), Some("1")),
                cell("30", &col, Some("2"), Some("1")),
            ],
            "with two duplicated keys",
        )
        .await;
        // A page that ends between a key's two entries counts the key whole,
        // once.
        for limit in [1, 3] {
            let walked = walk(&d, &kind.target(), limit).await;
            assert_eq!(
                walked.divergences,
                sorted(vec![
                    cell("20", &col, Some("2"), Some("1")),
                    cell("30", &col, Some("2"), Some("1")),
                ]),
                "{kind:?} at {limit}"
            );
        }
    }
}

// ------------------------------------------------------- changes in flight

/// Walks `target`'s parts without waiting for the pipeline, `limit` keys at a
/// time: the divergences, and whether any page could not vouch for every key.
async fn compare_walk(
    d: &Driver,
    target: &str,
    limit: i64,
) -> (Vec<Divergence>, bool, Vec<String>) {
    let mut after = None;
    let mut found = Vec::new();
    let (mut all, mut keys) = (false, Vec::new());
    loop {
        let page = compare_page(d.pool(), target, SelfCheckScope { after, limit })
            .await
            .expect("a page of the comparison");
        found.extend(page.divergences);
        all |= page.uncertain_all;
        keys.extend(page.uncertain_keys);
        after = page.next_after;
        if after.is_none() {
            return (sorted(found), all, keys);
        }
    }
}

/// Changes of both sides written and not yet applied (the ring holds them): a
/// move of an `a` row, an `a` row deleted and one inserted, a `b` row changed,
/// deleted and inserted on a key `a` rows hold, and a `b` key handed from one
/// row to another across two writes.
const PENDING: [&str; 8] = [
    "update public.a set k = 30, r = 1000 where id = 'a01'",
    "delete from public.a where id = 'a02'",
    "insert into public.a values ('a21', 60, 21, 's21')",
    "update public.b set img = 4242 where bid = 3",
    "delete from public.b where bid = 4",
    "insert into public.b values (7, 0, 700, 'k7')",
    "update public.b set k = 99 where bid = 5",
    "insert into public.b values (8, 50, 800, 'k8')",
];

#[tokio::test]
async fn changes_not_applied_yet_are_subtracted_on_every_part() {
    let _serial = SERIAL.lock().await;
    let mut d = start(true).await;
    for sql in PENDING {
        write(&d, sql).await;
    }
    for kind in KINDS {
        for limit in [1, 4, 1000] {
            let (found, all, keys) = compare_walk(&d, &kind.target(), limit).await;
            assert_eq!(found, Vec::new(), "{kind:?} at {limit}");
            assert!(
                !all && keys.is_empty(),
                "{kind:?} at {limit}: every pending change has images: {all} {keys:?}"
            );
        }
    }
    // Applied, they leave the same clean state.
    let batch = d.seal().await;
    d.drain(batch, "pending").await;
    assert_clean(&d, "after the pending changes are applied").await;
}

#[tokio::test]
async fn a_planted_error_beside_pending_changes_is_reported_exactly() {
    let _serial = SERIAL.lock().await;
    let d = start(true).await;
    for sql in PENDING {
        write(&d, sql).await;
    }
    for kind in KINDS {
        let t = kind.name();
        let h = hidden(&d, kind).await;
        // A row the pending changes don't touch, wrong in the target, in `R`
        // and in `M`.
        write(
            &d,
            &format!(
                "update public.{t} set total = total + 1 where id = 'a06'; \
                 update {} set r = r + 1 where __from_key = 'a09'; \
                 update {} set img = img + 1 where __from_key = '6'",
                h.r, h.m
            ),
        )
        .await;
        let (found, _, _) = compare_walk(&d, &kind.target(), 5).await;
        assert_eq!(
            found,
            sorted(vec![
                // a06 is on key 60: b6 (img 600), r = 6.
                cell("a06", "total", Some("607"), Some("606")),
                cell("a09", &format!("{}.r", h.r_name), Some("10"), Some("9")),
                cell("6", &format!("{}.img", h.m_name), Some("601"), Some("600")),
            ]),
            "{kind:?}"
        );
        write(
            &d,
            &format!(
                "update public.{t} set total = total - 1 where id = 'a06'; \
                 update {} set r = r - 1 where __from_key = 'a09'; \
                 update {} set img = img - 1 where __from_key = '6'",
                h.r, h.m
            ),
        )
        .await;
    }
}

/// An op that touches every key (a pending `TRUNCATE`) can't be subtracted: the
/// page says so, and `Standard` re-checks what it finds there.
#[tokio::test]
async fn a_pending_truncate_touches_every_key() {
    let _serial = SERIAL.lock().await;
    let d = start(true).await;
    write(&d, "truncate public.b").await;
    for kind in KINDS {
        let (_, all, _) = compare_walk(&d, &kind.target(), 1000).await;
        assert!(all, "{kind:?}: a pending TRUNCATE touches every key");
    }
}

/// A pending Re-derive has no images: its key is compared against the live row,
/// and named uncertain.
#[tokio::test]
async fn a_pending_recompute_of_a_names_its_key_uncertain() {
    let _serial = SERIAL.lock().await;
    let mut d = start(true).await;
    d.stage_recomputes("public.a", &["a04"]).await;
    for kind in KINDS {
        let (found, all, mut keys) = compare_walk(&d, &kind.target(), 1000).await;
        assert_eq!(found, Vec::new(), "{kind:?}");
        assert!(!all, "{kind:?}");
        keys.sort();
        keys.dedup();
        assert_eq!(keys, ["a04"], "{kind:?}");
    }
    let batch = d.seal().await;
    d.drain(batch, "recompute").await;
    assert_clean(&d, "after the Re-derive").await;
}

// ------------------------------------------------------ what is not compared

#[tokio::test]
async fn a_definition_that_is_still_building_is_not_compared() {
    let _serial = SERIAL.lock().await;
    let d = start(false).await;
    for kind in KINDS {
        let page = check_page(
            d.pool(),
            &kind.target(),
            SelfCheckScope {
                after: None,
                limit: 10,
            },
            SelfCheckMode::Standard,
            Duration::from_secs(30),
        )
        .await
        .expect("a page");
        assert!(
            matches!(page.outcome, SelfCheckOutcome::NotLive(_)),
            "{kind:?}: {:?}",
            page.outcome
        );
        assert_eq!(page.rows_compared, 0);
    }
}

// ------------------------------------------------- sides with a NULL key part

/// A side that is an aggregate's target, keyed by its nullable `GROUP BY`
/// column: some groups are keyed `NULL`.
async fn start_nullable(kind: Kind) -> Driver {
    set_enabled(true);
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, h integer, v integer); \
         create table public.b (id integer primary key, k integer, m integer, w integer); \
         create unique index on public.b (k, m); \
         insert into public.src values \
             (1, 1, 1, 10), (2, 1, null, 20), (3, null, 1, 5), (4, null, null, 7), \
             (5, 2, 2, 1), (6, 1, null, 3); \
         insert into public.b values (1, 1, 1, 100), (2, 2, 2, 200), (3, 1, null, 300), \
             (4, null, 1, 400)",
        &[
            ("g", ValueType::Integer(IntWidth::Int4)),
            ("h", ValueType::Integer(IntWidth::Int4)),
            ("v", ValueType::Integer(IntWidth::Int4)),
        ],
        &["TRANSFORM agg FROM public.src GROUP BY g, h WITHOUT SOURCE INDEX SELECT count(*) AS n"],
        &["public.src"],
    )
    .await;
    trellis::defs::install_definition(
        d.pool(),
        &format!(
            "TRANSFORM jn FROM public.agg {} JOIN public.b ON agg.g = b.k AND agg.h = b.m \
             SELECT agg.n + COALESCE(b.w, 0) AS total",
            kind.word()
        ),
        &HashMap::from([("total".to_string(), ValueType::Integer(IntWidth::Int8))]),
        "public",
    )
    .await
    .expect("define the 1-1 JOIN");
    let catalog = trellis::capture::columns::load_catalog(&d.ctl)
        .await
        .expect("load the catalog");
    let spec = trellis::capture::columns::capture_spec(&d.ctl, &catalog, "public.b")
        .await
        .expect("capture spec");
    match trellis::capture::install::reconcile(&mut d.ctl, DEFAULT_SCHEMA, &spec, None)
        .await
        .expect("capture b")
    {
        trellis::capture::install::Progress::Done(_) => {}
        trellis::capture::install::Progress::Waiting(wait) => panic!("capture waited: {wait}"),
    }
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    d
}

#[tokio::test]
async fn an_a_keyed_by_null_parts_is_compared_on_every_part() {
    let _serial = SERIAL.lock().await;
    for kind in KINDS {
        let mut d = start_nullable(kind).await;
        for limit in [1, 2, 100] {
            let walked = walk(&d, "public.jn", limit).await;
            assert_eq!(walked.divergences, Vec::new(), "{kind:?} at {limit}");
        }
        // The groups keyed `NULL` change, and the check follows them.
        step(
            &mut d,
            "insert into public.src values (7, null, 1, 1), (8, 1, null, 1)",
        )
        .await;
        step(
            &mut d,
            "delete from public.src where g is null and h is null",
        )
        .await;
        step(&mut d, "update public.b set w = 9 where id = 4").await;
        for limit in [1, 2, 100] {
            let walked = walk(&d, "public.jn", limit).await;
            assert_eq!(walked.divergences, Vec::new(), "{kind:?} at {limit}");
        }
        // Damage to a row keyed `NULL`, in the target and in `R`.
        let id: i64 = d
            .ctl
            .query_one(
                "select id from transform_definitions where target_table = 'public.jn'",
                &[],
            )
            .await
            .expect("id")
            .get(0);
        let r = format!("{DEFAULT_SCHEMA}.join1_{id}_r");
        let key: String = d
            .ctl
            .query_one(
                &format!(
                    "select __from_key from {r} where not __tombstone \
                     and (g is null or h is null) order by 1 limit 1"
                ),
                &[],
            )
            .await
            .expect("an entry keyed NULL")
            .get(0);
        write(&d, &format!("delete from {r} where __from_key = $${key}$$")).await;
        let walked = walk(&d, "public.jn", 2).await;
        assert!(
            walked.divergences.iter().any(|d| matches!(
                d,
                Divergence::Cell { key: k, column, persisted: None, recomputed: Some(live) }
                    if k == &key && column.ends_with("__entry") && live == "live"
            )),
            "{kind:?}: {:?}",
            walked.divergences
        );
    }
}

// ---------------------------------------------------- a chained 1-1 target side

#[tokio::test]
async fn a_chained_b_side_is_compared_with_the_images_of_its_upstream() {
    let _serial = SERIAL.lock().await;
    set_enabled(true);
    let mut d = Driver::start(
        "create table public.a (id integer primary key, k integer, x integer); \
         create table public.bsrc (id integer primary key, w integer); \
         insert into public.a values (1, 10, 1), (2, 10, 2), (3, 20, 3); \
         insert into public.bsrc values (10, 100), (20, 200)",
        &[("w", ValueType::Integer(IntWidth::Int4))],
        &["TRANSFORM tb FROM public.bsrc SELECT w AS w"],
        &["public.bsrc"],
    )
    .await;
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM jc FROM public.a LEFT JOIN public.tb ON a.k = tb.id \
         SELECT a.x + COALESCE(tb.w, 0) AS total",
        &HashMap::from([("total".to_string(), ValueType::Integer(IntWidth::Int4))]),
        "public",
    )
    .await
    .expect("define the join over the 1-1 target");
    let catalog = trellis::capture::columns::load_catalog(&d.ctl)
        .await
        .expect("load the catalog");
    let spec = trellis::capture::columns::capture_spec(&d.ctl, &catalog, "public.a")
        .await
        .expect("capture spec");
    match trellis::capture::install::reconcile(&mut d.ctl, DEFAULT_SCHEMA, &spec, None)
        .await
        .expect("capture a")
    {
        trellis::capture::install::Progress::Done(_) => {}
        trellis::capture::install::Progress::Waiting(wait) => panic!("capture waited: {wait}"),
    }
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    let clean = |d: &Driver, when: &'static str| {
        let pool = d.pool().clone();
        async move {
            let page = check_page(
                &pool,
                "public.jc",
                SelfCheckScope {
                    after: None,
                    limit: 1000,
                },
                SelfCheckMode::Standard,
                Duration::from_secs(30),
            )
            .await
            .expect("a page");
            assert_eq!(page.outcome, SelfCheckOutcome::Converged, "{when}");
        }
    };
    clean(&d, "at the start").await;
    for sql in [
        "update public.bsrc set w = 111 where id = 10",
        "delete from public.bsrc where id = 20",
        "insert into public.bsrc values (20, 222)",
        "update public.a set k = 20 where id = 1",
    ] {
        step(&mut d, sql).await;
        d.settle().await;
        // Walk every part: the one page ends where the walk does only at the
        // last part, so follow the cursor.
        let mut after = None;
        loop {
            let page = check_page(
                d.pool(),
                "public.jc",
                SelfCheckScope { after, limit: 2 },
                SelfCheckMode::Standard,
                Duration::from_secs(30),
            )
            .await
            .expect("a page");
            assert_eq!(page.outcome, SelfCheckOutcome::Converged, "after {sql}");
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
    }
    // A planted divergence in the target is found beside the chained side.
    write(&d, "update public.jc set total = total + 1 where id = 3").await;
    let page = check_page(
        d.pool(),
        "public.jc",
        SelfCheckScope {
            after: None,
            limit: 1000,
        },
        SelfCheckMode::Standard,
        Duration::from_secs(30),
    )
    .await
    .expect("a page");
    assert_eq!(
        page.outcome,
        SelfCheckOutcome::Diverged(vec![cell("3", "total", Some("226"), Some("225"))])
    );
}

// ------------------------------------------- a key of a non-default collation

/// Both tables keyed by a text column of an explicit collation (`und-x-icu`),
/// which the target key keeps, while `R`'s and `M`'s key text is `"C"`: every
/// comparison of the two names a collation, so neither the apply nor the check
/// fails to choose one. The keys sort differently under the two collations
/// (byte order puts the capitals first), so the pages split them where only the
/// key's own collation agrees.
#[tokio::test]
async fn a_key_of_a_non_default_collation_is_compared_on_every_part() {
    let _serial = SERIAL.lock().await;
    set_enabled(true);
    let mut d = Driver::start(
        "create table public.a (id text collate \"und-x-icu\" primary key, k integer, \
             r integer, s text); \
         create table public.b (bid text collate \"und-x-icu\" primary key, k integer, \
             img integer, k0 text); \
         create unique index on public.b (k); \
         create index on public.a (k); \
         insert into public.a \
             select (array['a', 'B', 'c', 'D'])[1 + i % 4] || i, i % 7 * 10, i, 's' || i \
             from generate_series(1, 24) i; \
         insert into public.b \
             select (array['x', 'Y'])[1 + i % 2] || i, i * 10, i * 100, 'k' || i \
             from generate_series(1, 6) i",
        &columns(),
        &[&Kind::Left.definition(), &Kind::Inner.definition()],
        &["public.a", "public.b"],
    )
    .await;
    // Changes of both sides drain: a moved key, a deleted and an inserted
    // row each, and a `b` key handed to another row.
    for sql in [
        "update public.a set k = 30, r = 0 where id = 'B1'",
        "delete from public.a where id = 'c2'",
        "insert into public.a values ('A99', 20, 99, 'x'), ('b98', 40, 98, 'y')",
        "update public.b set img = 1 where bid = 'x2'",
        "delete from public.b where bid = 'Y5'",
        "update public.b set k = 50 where bid = 'x6'",
    ] {
        step(&mut d, sql).await;
    }
    for kind in KINDS {
        let first = walk(&d, &kind.target(), 1000).await;
        assert_eq!(first.divergences, Vec::new(), "{kind:?}");
        for limit in [1, 3] {
            let walked = walk(&d, &kind.target(), limit).await;
            assert_eq!(walked.divergences, Vec::new(), "{kind:?} at {limit}");
            assert_eq!(walked.compared, first.compared, "{kind:?} at {limit}");
        }
        let h = hidden(&d, kind).await;
        let t = kind.name();
        // A planted error in the target, in `R` and in `M` is found.
        write(
            &d,
            &format!(
                "update public.{t} set total = total + 1 where id = 'B1'; \
                 update {} set r = r + 1 where __from_key = 'D3'; \
                 update {} set img = img + 1 where __from_key = 'x4'",
                h.r, h.m
            ),
        )
        .await;
        assert_found(
            &d,
            kind,
            vec![
                // B1 is on key 30 (`b`'s Y3, img 300), r = 0.
                cell("B1", "total", Some("301"), Some("300")),
                cell("D3", &format!("{}.r", h.r_name), Some("4"), Some("3")),
                cell("x4", &format!("{}.img", h.m_name), Some("401"), Some("400")),
            ],
            "with a planted error on each structure",
        )
        .await;
        write(
            &d,
            &format!(
                "update public.{t} set total = total - 1 where id = 'B1'; \
                 update {} set r = r - 1 where __from_key = 'D3'; \
                 update {} set img = img - 1 where __from_key = 'x4'",
                h.r, h.m
            ),
        )
        .await;
    }
    assert_clean(&d, "after the planted errors are undone").await;
}

// ------------------------------------------------------ through the public job

#[tokio::test]
async fn the_self_check_job_audits_a_1_1_join_target() {
    let _serial = SERIAL.lock().await;
    let d = start(true).await;
    let trellis = Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    for kind in KINDS {
        let report = trellis
            .self_check_to_end(
                &kind.target(),
                SelfCheckMode::Standard,
                Duration::from_secs(30),
            )
            .await
            .expect("the job");
        assert_eq!(report.outcome, SelfCheckOutcome::Converged, "{kind:?}");
        write(
            &d,
            &format!(
                "update public.{} set total = total + 1 where id = 'a02'",
                kind.name()
            ),
        )
        .await;
        let report = trellis
            .self_check_to_end(
                &kind.target(),
                SelfCheckMode::Standard,
                Duration::from_secs(30),
            )
            .await
            .expect("the job");
        assert_eq!(
            report.outcome,
            SelfCheckOutcome::Diverged(vec![cell("a02", "total", Some("203"), Some("202"))]),
            "{kind:?}"
        );
        write(
            &d,
            &format!(
                "update public.{} set total = total - 1 where id = 'a02'",
                kind.name()
            ),
        )
        .await;
    }
}
