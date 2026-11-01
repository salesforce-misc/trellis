//! A 1-1 `JOIN` whose source columns are re-collated after define, between
//! deterministic collations (#1567).
//!
//! The docs call such a change safe: byte equality is unchanged, so nothing
//! pauses. But the join's hidden tables (`R`, `M`, `J`) keep the collation
//! their columns were created with, and a statement that cast a source value
//! under the source column's collation, then met a hidden column, found two
//! implicit collations and no way to choose one (`42P22`). The first was the
//! lock of `J`'s keys, which failed every apply of a change to `a` once the
//! `ON` column of `a` was re-collated.
//!
//! One test per family of cells of the matrix ADR-0020 asks for: which column
//! was re-collated (`a`'s text `ON` column, `b`'s, both, a copied text column
//! that is not in the `ON`, the primary-key text of `a`, of `b`) crossed with
//! each statement that meets a hidden column (a page of either side, the
//! fan-out, a rebuild in place, a `PAUSE` and `RESUME`), for `LEFT` and
//! `INNER`. Each cell applies changes after the re-collation, compares the
//! target to the join over the source tables with the text compared by its
//! bytes, and then rebuilds in place and resumes.
//!
//! `self_check` runs the definition's own `JOIN ... ON` over the two source
//! tables, which meets the re-collated pair itself (#1564), so a cell that
//! leaves the `ON` pair at two collations is compared by the test's own
//! oracle query instead. The cells that leave the pair alone are also
//! checked.
//!
//! No test waits on convergence: changes are applied by sealing and draining
//! until nothing is pending, and builds are stepped by hand to idle.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::Config;
use trellis::defs::ValueType;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, Step, WorkerOptions};
use trellis::staging::self_check::{SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page};
use trellis::{Trellis, TrellisOptions};

/// The 1-1 `JOIN` tests turn the dev knob on, which is process-wide.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn options() -> WorkerOptions {
    WorkerOptions {
        chunk_rows: 3,
        drain_batch_cap: 10_000,
        heartbeat_interval: std::time::Duration::from_secs(1),
        reclaim_ttl: std::time::Duration::from_secs(60),
    }
}

/// Runs build steps until one is idle: a failed job is not claimable again
/// until its backoff passes, so a step that fails ends the loop too.
async fn work_to_idle(d: &Driver) {
    for _ in 0..200 {
        let step = build::work_once(d.pool(), "builder", &options())
            .await
            .expect("a build step");
        if matches!(step, Step::Idle) {
            return;
        }
    }
    panic!("the build never went idle");
}

async fn trellis(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

async fn status_and_error(d: &Driver, target: &str) -> (String, Option<String>) {
    let status: String = d
        .ctl
        .query_one(
            "select status::text from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("the definition")
        .get(0);
    let error: Option<String> = d
        .ctl
        .query_one(
            "select string_agg(last_error, '; ') from backfill_chunks c \
             join transform_definitions t on t.id = c.definition_id \
             where t.target_table = $1 and c.last_error is not null",
            &[&target],
        )
        .await
        .expect("the build's jobs")
        .get(0);
    (status, error)
}

/// A walk of `target`'s check, a page of one key and of every key, finds no
/// divergence.
async fn assert_converged(d: &Driver, target: &str, case: &str) {
    for limit in [1, 1000] {
        let mut after = None;
        loop {
            let page = check_page(
                d.pool(),
                target,
                SelfCheckScope { after, limit },
                SelfCheckMode::Standard,
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|e| panic!("{case}: a page of {target}'s check: {e}"));
            assert_eq!(
                page.outcome,
                SelfCheckOutcome::Converged,
                "{case}: {target} at {limit}"
            );
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
    }
}

/// Where each text column starts: its collation, and for the primary keys
/// whether the key is text at all (`None` is an `integer`).
#[derive(Clone, Copy)]
struct Layout {
    a_id: Option<&'static str>,
    b_id: Option<&'static str>,
    a_kt: &'static str,
    b_kt: &'static str,
    a_nm: &'static str,
    b_nm: &'static str,
}

const ICU: &str = "und-x-icu";

impl Layout {
    /// Every text column under ICU, the keys integers.
    const fn icu() -> Self {
        Layout {
            a_id: None,
            b_id: None,
            a_kt: ICU,
            b_kt: ICU,
            a_nm: ICU,
            b_nm: ICU,
        }
    }

    /// Every text column under ICU, the keys too.
    const fn text_keys() -> Self {
        Layout {
            a_id: Some(ICU),
            b_id: Some(ICU),
            ..Self::icu()
        }
    }

    fn id_type(id: Option<&str>) -> String {
        match id {
            Some(collation) => format!("text collate \"{collation}\""),
            None => "integer".to_string(),
        }
    }

    /// The expression of the id of row `i` of `a`, and of `b`.
    fn id_of(id: Option<&str>, i: &str) -> String {
        match id {
            Some(_) => format!("(array['a', 'B', 'c', 'D'])[1 + ({i}) % 4] || ({i})"),
            None => format!("({i})"),
        }
    }

    fn ddl(&self) -> String {
        format!(
            "create table public.a (id {a_id} primary key, kt text collate \"{a_kt}\", \
                 nm text collate \"{a_nm}\", v integer); \
             create table public.b (id {b_id} primary key, kt text collate \"{b_kt}\", \
                 nm text collate \"{b_nm}\", y integer); \
             create unique index on public.b (kt); create index on public.a (kt); \
             insert into public.a \
                 select {a_i}, 'k' || i % 6, (array['Ab', 'ab', 'Zq', 'zq'])[1 + i % 4] || i, i \
                 from generate_series(1, 12) i; \
             insert into public.b \
                 select {b_i}, 'k' || i, (array['Xy', 'xy'])[1 + i % 2] || i, i * 10 \
                 from generate_series(1, 4) i",
            a_id = Self::id_type(self.a_id),
            b_id = Self::id_type(self.b_id),
            a_kt = self.a_kt,
            b_kt = self.b_kt,
            a_nm = self.a_nm,
            b_nm = self.b_nm,
            a_i = Self::id_of(self.a_id, "i"),
            b_i = Self::id_of(self.b_id, "i"),
        )
    }

    fn columns(&self) -> Vec<(&'static str, ValueType)> {
        let id = if self.a_id.is_some() || self.b_id.is_some() {
            ValueType::Text
        } else {
            ValueType::Integer(IntWidth::Int4)
        };
        vec![
            ("id", id),
            ("kt", ValueType::Text),
            ("nm", ValueType::Text),
            ("v", ValueType::Integer(IntWidth::Int4)),
            ("y", ValueType::Integer(IntWidth::Int4)),
        ]
    }
}

fn alter(table: &str, column: &str, collation: &str) -> String {
    format!("alter table public.{table} alter column {column} type text collate \"{collation}\"")
}

/// One cell: the layout it starts from, the statements that re-collate it,
/// and whether `self_check` can read the result (it can when no `ON` pair is
/// left at two collations).
struct Cell {
    name: &'static str,
    layout: Layout,
    recollate: Vec<String>,
    /// The statements that bring the `ON` pair back to one collation, which a
    /// `RESUME` of a pair at two collations needs. Empty when the pair is
    /// still one.
    realign: Vec<String>,
}

impl Cell {
    fn checkable(&self) -> bool {
        self.realign.is_empty()
    }
}

fn definition(kind: &str) -> String {
    format!(
        "TRANSFORM t FROM public.a {kind} JOIN public.b ON a.kt = b.kt \
         SELECT b.y AS y, a.v AS v, a.nm AS anm, b.nm AS bnm"
    )
}

/// The join over the source tables, the `ON` text compared by its bytes, in
/// the target's columns.
fn oracle(kind: &str) -> String {
    format!(
        "select a.id, b.y, a.v, a.nm, b.nm from public.a a {kind} join public.b b \
         on a.kt = b.kt collate \"C\" order by a.id::text collate \"C\""
    )
}

const TARGET: &str = "select id, y, v, anm, bnm from public.t order by id::text collate \"C\"";

async fn assert_matches(d: &Driver, kind: &str, case: &str, stage: &str) {
    assert_eq!(
        d.rows(TARGET).await,
        d.rows(&oracle(kind)).await,
        "{case}: {stage}"
    );
}

/// Changes of `a`: updates of read columns, `ON` keys moved to and from a
/// match, inserts and deletes.
fn a_changes(layout: &Layout, round: i32) -> String {
    let a_new = Layout::id_of(layout.a_id, &format!("v + {}", 100 * round));
    let (kt_hit, kt_new) = (format!("k{}", round % 4 + 1), format!("k{}", 20 + round));
    format!(
        "update public.a set v = v + {round}00 where v % 3 = 0; \
         update public.a set kt = '{kt_new}' where v % 7 = 1; \
         update public.a set nm = nm || 'z' where v % 5 = 0; \
         delete from public.a where v in (4, 9); \
         insert into public.a select {a_new}, '{kt_hit}', nm, v + {round}000 from public.a \
             where v < 6"
    )
}

/// Changes of `b`: updates, a key moved (the fan-out reaches the rows of `a`
/// on the key it left and the key it took), inserts and deletes.
fn b_changes(layout: &Layout, round: i32) -> String {
    let b_new = Layout::id_of(layout.b_id, &format!("y + {}", 1000 * round));
    let kt_new = format!("k{}", 20 + round);
    format!(
        "update public.b set y = y + 1 where y < 30; \
         update public.b set kt = '{kt_new}' where y = 20 or y = 21; \
         update public.b set nm = nm || 'q' where y = 40; \
         insert into public.b select {b_new}, 'k{r5}', nm, 77 + {round} from public.b \
             where y = 10 or y = 11; \
         delete from public.b where y = 30 or y = 31",
        r5 = 5 + round,
    )
}

/// Applies `sql` and drains until nothing is pending.
async fn apply(d: &mut Driver, name: &str, sql: &str) {
    d.user()
        .await
        .batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{name}: {sql}: {e}"));
    d.settle().await;
}

/// Changes to both tables, one side then the other, each applied by its own
/// pages.
async fn apply_changes(d: &mut Driver, layout: &Layout, round: i32, name: &str) {
    apply(d, name, &a_changes(layout, round)).await;
    apply(d, name, &b_changes(layout, round)).await;
}

/// Builds `cell`, re-collates, applies changes, rebuilds in place, pauses and
/// resumes, comparing the target to the oracle after each.
async fn run_cell(cell: &Cell, kind: &str) {
    let name = format!("{} ({kind})", cell.name);
    let name = name.as_str();
    set_enabled(true);
    let mut d = Driver::start(
        &cell.layout.ddl(),
        &cell.layout.columns(),
        &[&definition(kind)],
        &["public.a", "public.b"],
    )
    .await;
    assert_matches(&d, kind, name, "built").await;
    for stmt in &cell.recollate {
        d.ctl
            .batch_execute(stmt)
            .await
            .unwrap_or_else(|e| panic!("{name}: {stmt}: {e}"));
    }
    assert_matches(&d, kind, name, "re-collated").await;

    // Changes to both tables, applied after the re-collation.
    apply_changes(&mut d, &cell.layout, 1, name).await;
    assert_matches(&d, kind, name, "applied").await;
    if cell.checkable() {
        assert_converged(&d, "public.t", name).await;
    }

    // A rebuild in place: the sweep and its orphan walk.
    let orphan = Layout::id_of(cell.layout.a_id, "1000");
    d.ctl
        .batch_execute(&format!(
            "insert into public.t (id, y, v, anm, bnm) values ({orphan}, 1, 1, 'o', 'o')"
        ))
        .await
        .unwrap_or_else(|e| panic!("{name}: an orphan: {e}"));
    let trellis = trellis(&d).await;
    trellis
        .request_backfill("public.a")
        .await
        .unwrap_or_else(|e| panic!("{name}: request a rebuild: {e}"));
    work_to_idle(&d).await;
    let (status, error) = status_and_error(&d, "public.t").await;
    assert_eq!((status.as_str(), error), ("live", None), "{name}: rebuilt");
    assert_matches(&d, kind, name, "rebuilt").await;

    // A PAUSE, changes while paused, and a RESUME.
    trellis
        .apply("PAUSE TRANSFORM t")
        .await
        .unwrap_or_else(|e| panic!("{name}: pause: {e}"));
    apply_changes(&mut d, &cell.layout, 2, name).await;
    if !cell.realign.is_empty() {
        // A resume re-validates the definition as define does, and refuses
        // an `ON` pair at two collations.
        assert!(
            trellis.apply("RESUME TRANSFORM t").await.is_err(),
            "{name}: a resume with the pair at two collations is refused"
        );
        for stmt in &cell.realign {
            d.ctl
                .batch_execute(stmt)
                .await
                .unwrap_or_else(|e| panic!("{name}: {stmt}: {e}"));
        }
    }
    trellis
        .apply("RESUME TRANSFORM t")
        .await
        .unwrap_or_else(|e| panic!("{name}: resume: {e}"));
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the resumed builds");
    work_to_idle(&d).await;
    let (status, error) = status_and_error(&d, "public.t").await;
    assert_eq!((status.as_str(), error), ("live", None), "{name}: resumed");
    assert_matches(&d, kind, name, "resumed").await;
    assert_converged(&d, "public.t", name).await;

    // Changes after the resume run against the recreated hidden tables.
    apply_changes(&mut d, &cell.layout, 3, name).await;
    assert_matches(&d, kind, name, "applied after the resume").await;
    assert_converged(&d, "public.t", name).await;

    // A TRUNCATE of either table empties its ledger, and one of `b` starts a
    // rebuild of the target.
    apply(&mut d, name, "truncate public.b").await;
    work_to_idle(&d).await;
    assert_matches(&d, kind, name, "b truncated").await;
    let b_new = Layout::id_of(cell.layout.b_id, "5");
    apply(
        &mut d,
        name,
        &format!("insert into public.b select {b_new}, 'k3', 'Xy', 55; truncate public.a"),
    )
    .await;
    work_to_idle(&d).await;
    assert_matches(&d, kind, name, "a truncated").await;
    let a_new = Layout::id_of(cell.layout.a_id, "7");
    apply(
        &mut d,
        name,
        &format!("insert into public.a select {a_new}, 'k3', 'Zq', 7"),
    )
    .await;
    assert_matches(&d, kind, name, "a inserted after the truncates").await;
}

async fn run(cell: Cell) {
    let _serial = SERIAL.lock().await;
    for kind in ["", "LEFT"] {
        run_cell(&cell, kind).await;
    }
}

/// The issue's repro: `a`'s `ON` column ICU to POSIX, `b`'s untouched.
#[tokio::test]
async fn the_from_tables_on_column_re_collated() {
    run(Cell {
        name: "a.kt icu then POSIX",
        layout: Layout::icu(),
        recollate: vec![alter("a", "kt", "POSIX")],
        realign: vec![alter("b", "kt", "POSIX")],
    })
    .await;
}

/// The joined table's `ON` column re-collated: its keys sit in `M` and `J`
/// with the collation they were created with.
#[tokio::test]
async fn the_joined_tables_on_column_re_collated() {
    run(Cell {
        name: "b.kt icu then POSIX",
        layout: Layout::icu(),
        recollate: vec![alter("b", "kt", "POSIX")],
        realign: vec![alter("a", "kt", "POSIX")],
    })
    .await;
}

/// Both sides' `ON` columns re-collated, to one collation and to two, and
/// from and to the database's default.
#[tokio::test]
async fn both_on_columns_re_collated() {
    run(Cell {
        name: "a.kt and b.kt icu then POSIX",
        layout: Layout::icu(),
        recollate: vec![alter("a", "kt", "POSIX"), alter("b", "kt", "POSIX")],
        realign: vec![],
    })
    .await;
    run(Cell {
        name: "a.kt POSIX then C, b.kt POSIX then icu",
        layout: Layout {
            a_kt: "POSIX",
            b_kt: "POSIX",
            ..Layout::icu()
        },
        recollate: vec![alter("a", "kt", "C"), alter("b", "kt", ICU)],
        realign: vec![alter("b", "kt", "C")],
    })
    .await;
}

/// `ON` columns of the database's default collation, and of `C`, re-collated.
#[tokio::test]
async fn on_columns_of_the_default_collation_and_of_c_re_collated() {
    run(Cell {
        name: "a.kt and b.kt default then POSIX",
        layout: Layout {
            a_kt: "default",
            b_kt: "default",
            ..Layout::icu()
        },
        recollate: vec![alter("a", "kt", "POSIX"), alter("b", "kt", "POSIX")],
        realign: vec![],
    })
    .await;
    run(Cell {
        name: "a.kt C then icu, b.kt C",
        layout: Layout {
            a_kt: "C",
            b_kt: "C",
            ..Layout::icu()
        },
        recollate: vec![alter("a", "kt", ICU)],
        realign: vec![alter("b", "kt", ICU)],
    })
    .await;
}

/// A copied text column that is not in the `ON`, re-collated on either side:
/// `R` and `M` hold it under the collation it was created with.
#[tokio::test]
async fn a_copied_text_column_re_collated() {
    run(Cell {
        name: "a.nm and b.nm icu then POSIX",
        layout: Layout::icu(),
        recollate: vec![alter("a", "nm", "POSIX"), alter("b", "nm", "POSIX")],
        realign: vec![],
    })
    .await;
    run(Cell {
        name: "a.nm icu then POSIX",
        layout: Layout::icu(),
        recollate: vec![alter("a", "nm", "POSIX")],
        realign: vec![],
    })
    .await;
    run(Cell {
        name: "b.nm icu then POSIX",
        layout: Layout::icu(),
        recollate: vec![alter("b", "nm", "POSIX")],
        realign: vec![],
    })
    .await;
}

/// The primary key of `a` is text, re-collated: the target's key column and
/// `R`'s key text keep what they were created with.
#[tokio::test]
async fn the_from_tables_primary_key_text_re_collated() {
    run(Cell {
        name: "a.id icu then POSIX",
        layout: Layout::text_keys(),
        recollate: vec![alter("a", "id", "POSIX")],
        realign: vec![],
    })
    .await;
}

/// The primary key of `b` is text, re-collated: `M`'s key text.
#[tokio::test]
async fn the_joined_tables_primary_key_text_re_collated() {
    run(Cell {
        name: "b.id icu then POSIX",
        layout: Layout::text_keys(),
        recollate: vec![alter("b", "id", "POSIX")],
        realign: vec![],
    })
    .await;
}

// ------------------------------------------------------------- quarantine

fn quarantine_definition(kind: &str) -> String {
    format!(
        "TRANSFORM t FROM public.a {kind} JOIN public.b ON a.kt = b.kt \
         SELECT a.v + COALESCE(b.y, 0) AS total, b.nm AS bnm"
    )
}

/// The target's rows as the live join computes them, but the `held` ids.
fn quarantine_oracle(kind: &str, held: &str) -> String {
    format!(
        "select a.id, a.v + coalesce(b.y, 0), b.nm from public.a a {kind} join public.b b \
         on a.kt = b.kt collate \"C\" where a.id::text <> all('{{{held}}}'::text[]) \
         order by a.id::text collate \"C\""
    )
}

async fn assert_all_but(d: &Driver, kind: &str, held: &str, case: &str, stage: &str) {
    assert_eq!(
        d.rows(&format!(
            "select id, total, bnm from public.t where id::text <> all('{{{held}}}'::text[]) \
             order by id::text collate \"C\""
        ))
        .await,
        d.rows(&quarantine_oracle(kind, held)).await,
        "{case}: {stage}"
    );
}

async fn step(d: &mut Driver, sql: &str) {
    d.user()
        .await
        .batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

/// A row held in quarantine after a re-collation, by a `b` change's fan-out
/// and by its own page, and its release: the held-row checks and the
/// release's re-derive meet `R`, `M` and `J` as a page does.
async fn a_held_row_is_released_after_a_re_collation(cell: Cell) {
    let _serial = SERIAL.lock().await;
    for kind in ["", "LEFT"] {
        let case = format!("{} ({kind})", cell.name);
        let case = case.as_str();
        set_enabled(true);
        let mut d = Driver::start(
            &cell.layout.ddl(),
            &cell.layout.columns(),
            &[&quarantine_definition(kind)],
            &["public.a", "public.b"],
        )
        .await;
        for stmt in &cell.recollate {
            d.ctl
                .batch_execute(stmt)
                .await
                .unwrap_or_else(|e| panic!("{case}: {stmt}: {e}"));
        }
        let new_id = Layout::id_of(cell.layout.a_id, "100");
        step(
            &mut d,
            &format!("insert into public.a select {new_id}, 'k1', 'n', 2147483000"),
        )
        .await;
        let key: String = d
            .ctl
            .query_one("select id::text from public.a where v = 2147483000", &[])
            .await
            .expect("the big row")
            .get(0);
        assert_all_but(&d, kind, "", case, "the big row").await;

        // The fan-out of a `b` change overflows the big row's `total`.
        step(&mut d, "update public.b set y = 1000 where kt = 'k1'").await;
        assert_all_but(&d, kind, &key, case, "the fan-out").await;
        assert_eq!(
            d.rows("select src_table, key from poison").await,
            [format!("(public.a,{key})")],
            "{case}: the row is held"
        );
        // Its own change while held is parked.
        step(&mut d, "update public.a set v = 5 where v = 2147483000").await;
        assert_all_but(&d, kind, &key, case, "the parked change").await;
        let held = trellis::staging::quarantine::release_key(d.pool(), "t", "public.a", &key)
            .await
            .expect("release the key");
        assert_eq!(held, 1, "{case}: the parked work");
        let batch = d.seal().await;
        d.drain(batch, "release").await;
        assert_all_but(&d, kind, "", case, "the release").await;
        assert!(d.rows("select key from poison").await.is_empty(), "{case}");

        // Its own page overflows: the page holds the row.
        step(&mut d, "update public.a set v = 2147483600 where v = 5").await;
        assert_all_but(&d, kind, &key, case, "the page's own hold").await;
        step(&mut d, "update public.a set v = 6 where v = 2147483600").await;
        let held = trellis::staging::quarantine::release_key(d.pool(), "t", "public.a", &key)
            .await
            .expect("release the key");
        assert_eq!(held, 1, "{case}: the parked work");
        let batch = d.seal().await;
        d.drain(batch, "release").await;
        assert_all_but(&d, kind, "", case, "the second release").await;
    }
}

#[tokio::test]
async fn a_held_row_is_released_after_the_from_tables_on_column_is_re_collated() {
    a_held_row_is_released_after_a_re_collation(Cell {
        name: "quarantine, a.kt icu then POSIX",
        layout: Layout::icu(),
        recollate: vec![alter("a", "kt", "POSIX")],
        realign: vec![],
    })
    .await;
}

#[tokio::test]
async fn a_held_row_is_released_after_a_text_primary_key_is_re_collated() {
    a_held_row_is_released_after_a_re_collation(Cell {
        name: "quarantine, a.id and a.kt icu then POSIX",
        layout: Layout::text_keys(),
        recollate: vec![alter("a", "id", "POSIX"), alter("a", "kt", "POSIX")],
        realign: vec![],
    })
    .await;
}
