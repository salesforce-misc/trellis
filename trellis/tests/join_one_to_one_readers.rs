//! Readers of a 1-1 `JOIN` target (#1617, rule 4): every writer of the
//! target stages its changes for the target's readers in its own
//! transaction. A page's writes of its `a` changes' rows and of its inline
//! fan-out, and each fan-out chunk (with its marker's origin), are the first
//! scenarios; the build's writers (its chunks, the rebuild sweep's stale and
//! orphan passes, a quarantine release's Re-derive) and the writers that
//! reach the target through a page or a build (a `TRUNCATE` of either side,
//! a producer's Re-derives, a `RESUME`) the second.
//!
//! The readers are defined through define, as any transform is. The tables
//! start empty and every row arrives through pages; the build-writer
//! scenarios then write past capture, or pause the transform, so only the
//! writer under test finds the change.
//!
//! Four readers read the target: an aggregate on the group basis, which
//! applies each change's images as a delta, a 1-1 transform, an aggregate of
//! that 1-1 transform (a chain), and a 1-1 `JOIN` of `a` with the target on
//! its primary key (the target as a `b` side). The chunk is shrunk to two
//! rows over a key of seven `a` rows, and pages and chunks are driven by hand
//! (`support/drain_driver.rs`), frozen at a [`PausePoint`] where a scenario
//! needs it. Nothing sleeps or polls for convergence. Each scenario runs on
//! both join kinds.
//!
//! - a reader's page races a chunk both ways: a chunk frozen before its
//!   commit while the reader drains the rows the last chunk staged, and the
//!   reader's page frozen before its commit while the next chunk commits;
//! - a chunk killed after it staged its rows leaves nothing in the ring, and
//!   the next chunk stages them again;
//! - a second `b` change resets the cursor while the readers' rows from the
//!   first change's chunks are half staged (some drained, some still in the
//!   ring);
//!
//! - a rebuild in place, a `TRUNCATE` of `a`, a `TRUNCATE` of `b` and its
//!   rebuild, a producer's Re-derives, a `RESUME`'s rebuild and a quarantine
//!   release, each run to its end;
//! - a widening of a column the target copies, whose `RESUME` pauses and
//!   resumes the readers down the chain;
//!
//! each ending with every reader equal to its source, `self_check` clean on
//! the target and every reader, and the token taken after the scenario's
//! writes converged. A reader's own field failing on a row the target holds
//! holds that key for that reader alone, as for a reader of any target.

#[path = "support/attver.rs"]
mod attver;
#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
use std::time::Duration;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::dev::defs::set_one_to_one_join_fan_out_chunk_rows;
use trellis::integer::IntWidth;
use trellis::staging::OneToOneJoinFanOutStep;
use trellis::staging::interleave::PausePoint;
use trellis::staging::self_check::{SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page};

const DDL: &str = "create table public.a (id integer primary key, k integer, x integer, \
                       y text); \
                   create table public.b (id integer primary key, k integer, w integer, \
                       tag text); \
                   create unique index on public.b (k); \
                   create index on public.a (k)";

/// Seven `a` rows on key 10 (ids 1..7, more than three chunks), one on key 20
/// and one on no key; a `b` row on each key.
const A_ROWS: &str = "(1, 10, 1, 'one'), (2, 10, 2, 'two'), (3, 10, 3, 'three'), \
                      (4, 10, 4, 'four'), (5, 10, 5, 'five'), (6, 10, 6, 'six'), \
                      (7, 10, 7, 'seven'), (8, 20, 8, 'eight'), (9, null, 9, 'nine')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty')";

/// The chunk these tests run with.
const CHUNK: usize = 2;

/// The chunk is process-wide, so these tests run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Holds the serial lock with the chunk at [`CHUNK`], and puts it back.
struct Chunked {
    _serial: tokio::sync::MutexGuard<'static, ()>,
}

impl Drop for Chunked {
    fn drop(&mut self) {
        set_one_to_one_join_fan_out_chunk_rows(0);
    }
}

async fn chunked() -> Chunked {
    let serial = SERIAL.lock().await;
    set_one_to_one_join_fan_out_chunk_rows(CHUNK);
    Chunked { _serial: serial }
}

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

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

    /// The aggregate reader's target.
    fn sums(self) -> String {
        format!("public.{}_sums", self.name())
    }

    /// The 1-1 reader's target.
    fn copy(self) -> String {
        format!("public.{}_copy", self.name())
    }

    /// The reader of the 1-1 reader: an aggregate.
    fn chain(self) -> String {
        format!("public.{}_chain", self.name())
    }

    /// The `LEFT` 1-1 `JOIN` of `a` with the target, on its primary key.
    fn over(self) -> String {
        format!("public.{}_over", self.name())
    }

    /// Every target the scenarios keep exact.
    fn targets(self) -> [String; 5] {
        [
            self.target(),
            self.sums(),
            self.copy(),
            self.chain(),
            self.over(),
        ]
    }
}

/// `kind` and its readers over empty tables, built and live, `a` and `b`
/// captured; then `a`'s rows and `b`'s, each a page of its own, and
/// everything settled. The `b` page fans key 20 out itself and leaves key 10
/// to chunks. The readers: an aggregate and a 1-1 transform of the target, an
/// aggregate of that 1-1 reader (a chain), and a 1-1 `JOIN` of `a` with the
/// target on its primary key (the target as a `b` side).
async fn start(kind: Kind) -> Driver {
    let columns = [
        ("id", int4()),
        ("k", int4()),
        ("x", int4()),
        ("y", ValueType::Text),
        ("total", int4()),
        ("tag", ValueType::Text),
    ];
    let mut d = Driver::start(
        DDL,
        &columns,
        &[&format!(
            "TRANSFORM {} FROM public.a {} JOIN public.b ON a.k = b.k \
             SELECT a.x + COALESCE(b.w, 0) AS total, b.tag AS tag, a.y AS y",
            kind.name(),
            kind.word()
        )],
        &["public.a", "public.b"],
    )
    .await;
    // A reader is defined over a live target; the aggregate's `GROUP BY`
    // column needs an index on it.
    d.ctl
        .batch_execute(&format!("create index on {} (tag)", kind.target()))
        .await
        .expect("index the target's tag");
    let columns: HashMap<String, ValueType> = columns
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect();
    for reader in [
        format!(
            "TRANSFORM {}_sums FROM {} GROUP BY tag \
             SELECT tag AS tag, SUM(total) AS s, COUNT(*) AS n",
            kind.name(),
            kind.target()
        ),
        format!(
            "TRANSFORM {}_copy FROM {} SELECT total - 1 AS less, tag AS tag",
            kind.name(),
            kind.target()
        ),
        format!(
            "TRANSFORM {}_chain FROM {} GROUP BY tag \
             SELECT tag AS tag, SUM(less) AS s, COUNT(*) AS n",
            kind.name(),
            kind.copy()
        ),
        format!(
            "TRANSFORM {name}_over FROM public.a LEFT JOIN {target} ON a.id = {name}.id \
             SELECT {name}.total AS t, a.y AS y",
            name = kind.name(),
            target = kind.target()
        ),
    ] {
        let mut columns = columns.clone();
        if reader.contains("_chain") {
            d.ctl
                .batch_execute(&format!("create index on {} (tag)", kind.copy()))
                .await
                .expect("index the 1-1 reader's tag");
            columns.insert("less".to_string(), int4());
        }
        trellis::defs::install_definition(d.pool(), &reader, &columns, "public")
            .await
            .unwrap_or_else(|err| panic!("define a reader: {reader}: {err}"));
        // The chain reads the 1-1 reader, which must be live first.
        trellis::intake::harness::settle_builds(d.pool()).await;
    }
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    step(&mut d, &format!("insert into public.a values {A_ROWS}")).await;
    step(&mut d, &format!("insert into public.b values {B_ROWS}")).await;
    assert_eq!(markers(&d, kind).await, ["({10},1,)"], "{kind:?}");
    assert_exact(&mut d, kind, "the seed").await;
    d
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it as one sealed batch, running no chunk.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

/// Seals and drains what the ring holds, running no chunk.
async fn drain_ring(d: &mut Driver) {
    let batch = d.seal().await;
    d.drain(batch, "readers").await;
}

/// `kind`'s markers, as `(join key, gen, cursor)`.
async fn markers(d: &Driver, kind: Kind) -> Vec<String> {
    d.rows(&format!(
        "select p.join_key, p.gen, p.cursor from join1_fan_out p \
         join transform_definitions d on d.id = p.transform_id \
         where d.target_table = '{}' order by p.join_key",
        kind.target()
    ))
    .await
}

/// The ring rows the seam staged for `kind`'s target that no drain has
/// applied yet (in a slot whose segment isn't drained), as their origins.
async fn staged(d: &Driver, kind: Kind) -> Vec<String> {
    let slots: Vec<String> = (0..4)
        .map(|slot| {
            format!(
                "select origin_lsn, src_table from seg_{slot} where exists \
                 (select 1 from segments where ring_slot = {slot} and state <> 'drained')"
            )
        })
        .collect();
    d.rows(&format!(
        "select origin_lsn from ({}) r where src_table = '{}' order by 1",
        slots.join(" union all "),
        kind.target()
    ))
    .await
}

/// The aggregate reader's rows, as `(tag, s, n)`.
async fn sums(d: &Driver, kind: Kind) -> Vec<String> {
    d.rows(&format!(
        "select tag, s, n from {} order by tag",
        kind.sums()
    ))
    .await
}

/// What the aggregate reader holds when it equals its source.
async fn sums_of_source(d: &Driver, kind: Kind) -> Vec<String> {
    d.rows(&format!(
        "select tag, sum(total), count(*) from {} group by tag order by tag",
        kind.target()
    ))
    .await
}

/// Settles everything, then asserts `kind`'s target equals the live join and
/// each reader equals its source, with no marker left and a token taken
/// before the settle converged.
async fn assert_exact(d: &mut Driver, kind: Kind, after: &str) {
    let token = token(d).await;
    d.settle().await;
    assert!(
        converged(d, token).await,
        "{kind:?}: the token converged after {after}"
    );
    assert!(
        markers(d, kind).await.is_empty(),
        "{kind:?}: no marker after {after}"
    );
    let target = d
        .rows(&format!(
            "select id, total, tag, y from {} order by id",
            kind.target()
        ))
        .await;
    let join = d
        .rows(&format!(
            "select a.id, a.x + coalesce(b.w, 0), b.tag, a.y from public.a a {} join public.b b \
             on a.k = b.k order by a.id",
            kind.word()
        ))
        .await;
    assert_eq!(
        target, join,
        "{kind:?} target (left) differs from the live join (right) after {after}"
    );
    assert_eq!(
        sums(d, kind).await,
        sums_of_source(d, kind).await,
        "{kind:?}: the aggregate reader (left) differs from its source (right) after {after}"
    );
    let copy = d
        .rows(&format!(
            "select id, less, tag from {} order by id",
            kind.copy()
        ))
        .await;
    let copy_source = d
        .rows(&format!(
            "select id, total - 1, tag from {} order by id",
            kind.target()
        ))
        .await;
    assert_eq!(
        copy, copy_source,
        "{kind:?}: the 1-1 reader (left) differs from its source (right) after {after}"
    );
    let chain = d
        .rows(&format!(
            "select tag, s, n from {} order by tag",
            kind.chain()
        ))
        .await;
    let chain_source = d
        .rows(&format!(
            "select tag, sum(less), count(*) from {} group by tag order by tag",
            kind.copy()
        ))
        .await;
    assert_eq!(
        chain, chain_source,
        "{kind:?}: the chained reader (left) differs from its source (right) after {after}"
    );
    let over = d
        .rows(&format!("select id, t, y from {} order by id", kind.over()))
        .await;
    let over_source = d
        .rows(&format!(
            "select a.id, j.total, a.y from public.a a left join {} j on a.id = j.id \
             order by a.id",
            kind.target()
        ))
        .await;
    assert_eq!(
        over, over_source,
        "{kind:?}: the 1-1 JOIN over the target (left) differs from its join (right) after \
         {after}"
    );
    for target in kind.targets() {
        assert_self_check_clean(d, &target, after).await;
    }
}

/// Every page of `target`'s `self_check` comparison finds nothing.
async fn assert_self_check_clean(d: &Driver, target: &str, after: &str) {
    let mut scope_after = None;
    loop {
        let page = check_page(
            d.pool(),
            target,
            SelfCheckScope {
                after: scope_after,
                limit: 3,
            },
            SelfCheckMode::Standard,
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|err| panic!("a page of {target}'s check after {after}: {err}"));
        assert_eq!(
            page.outcome,
            SelfCheckOutcome::Converged,
            "{target}: self_check after {after}"
        );
        assert!(
            page.held_keys.is_none(),
            "{target}: no held key after {after}: {:?}",
            page.held_keys
        );
        scope_after = page.next_after;
        if scope_after.is_none() {
            return;
        }
    }
}

/// The current WAL insert position: a watermark token for every commit so far.
async fn token(d: &Driver) -> tokio_postgres::types::PgLsn {
    d.ctl
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await
        .expect("the WAL position")
        .get(0)
}

async fn converged(d: &Driver, token: tokio_postgres::types::PgLsn) -> bool {
    trellis::staging::converge::converged_through(&d.ctl, token)
        .await
        .expect("converged_through")
}

/// Ends a frozen chunk's session and waits until it has exited, so its
/// transaction has rolled back and released its locks.
async fn end_session(d: &Driver, pid: i32) {
    let ended: bool = d
        .ctl
        .query_one("select pg_terminate_backend($1, 60000)", &[&pid])
        .await
        .expect("end the chunk's session")
        .get(0);
    assert!(ended, "the chunk's session {pid} exited");
}

/// Runs one chunk step and asserts it ran a chunk.
async fn one_chunk(d: &Driver) {
    let step = trellis::staging::one_to_one_join_fan_out_step(d.pool())
        .await
        .expect("a chunk");
    assert_eq!(step, OneToOneJoinFanOutStep::Chunk);
}

/// Starts one chunk step in its own task, frozen at `point` for `kind`.
async fn frozen_chunk(
    d: &Driver,
    kind: Kind,
    point: PausePoint,
) -> drain_driver::Running<OneToOneJoinFanOutStep> {
    let target = kind.target();
    d.run_frozen(&[(point, target.as_str())], |pool| async move {
        trellis::staging::one_to_one_join_fan_out_step(&pool).await
    })
    .await
}

/// The marker's origin, as its text.
async fn marker_origin(d: &Driver) -> Vec<String> {
    d.rows("select origin_lsn from join1_fan_out").await
}

/// A reader's page races a chunk. First a chunk is frozen after its writes
/// and its staging, before its commit, while the readers drain the rows the
/// chunk before it staged: they apply them, and see key 10 half joined to
/// the new `b` row, which the read contract allows. Then a reader's page is
/// frozen before its commit while the next chunk commits beside it. Each
/// chunk's staged rows carry the marker's origin.
#[tokio::test]
async fn a_readers_page_races_a_chunk() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        step(
            &mut d,
            "update public.b set w = 900, tag = 'new' where id = 1",
        )
        .await;
        let origin = marker_origin(&d).await;
        assert_eq!(origin.len(), 1, "{kind:?}");
        one_chunk(&d).await;
        assert_eq!(
            staged(&d, kind).await,
            vec![origin[0].clone(); CHUNK],
            "{kind:?}: the chunk staged its two rows with the marker's origin"
        );
        let batch = d.seal().await;

        // A chunk frozen before its commit while the readers drain the last
        // chunk's rows.
        let mut chunk = frozen_chunk(&d, kind, PausePoint::BeforeJoin1FanOutCommit).await;
        chunk.reached(PausePoint::BeforeJoin1FanOutCommit).await;
        d.drain(batch, "readers").await;
        assert_eq!(
            sums(&d, kind).await,
            sums_of_source(&d, kind).await,
            "{kind:?}: the aggregate reader applied the committed chunk's rows"
        );
        assert!(
            sums(&d, kind)
                .await
                .iter()
                .any(|row| row.starts_with("(new,"))
                && sums(&d, kind)
                    .await
                    .iter()
                    .any(|row| row.starts_with("(ten,")),
            "{kind:?}: mid fan-out, key 10 joins the old b row and the new: {:?}",
            sums(&d, kind).await
        );
        d.release(&mut chunk, PausePoint::BeforeJoin1FanOutCommit)
            .await;
        assert_eq!(chunk.finish().await, OneToOneJoinFanOutStep::Chunk);
        assert_eq!(markers(&d, kind).await, ["({10},1,4)"], "{kind:?}");

        // A reader's page frozen before its commit while the next chunk
        // commits.
        let batch = d.seal().await;
        let sums_target = kind.sums();
        let mut page = d
            .drain_frozen(
                batch,
                "readers",
                &[(PausePoint::BeforeCommit, sums_target.as_str())],
            )
            .await;
        page.reached(PausePoint::BeforeCommit).await;
        one_chunk(&d).await;
        assert_eq!(markers(&d, kind).await, ["({10},1,6)"], "{kind:?}");
        d.release(&mut page, PausePoint::BeforeCommit).await;
        page.finish().await;
        assert_exact(&mut d, kind, "a reader's page racing a chunk").await;
    }
}

/// A chunk killed after it staged its rows, before its commit, leaves
/// nothing in the ring: the readers have nothing to apply from it, and the
/// next chunk writes and stages the same rows again.
#[tokio::test]
async fn a_chunk_killed_after_the_seam_staged_nothing() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        step(&mut d, "update public.b set w = 900 where id = 1").await;
        assert!(staged(&d, kind).await.is_empty(), "{kind:?}");
        let before = sums(&d, kind).await;
        let mut chunk = frozen_chunk(&d, kind, PausePoint::BeforeJoin1FanOutCommit).await;
        let reached = chunk.reached(PausePoint::BeforeJoin1FanOutCommit).await;
        chunk.kill().await;
        end_session(&d, reached.backend_pid).await;
        assert!(
            staged(&d, kind).await.is_empty(),
            "{kind:?}: the killed chunk staged nothing"
        );
        assert_eq!(markers(&d, kind).await, ["({10},1,)"], "{kind:?}");
        drain_ring(&mut d).await;
        assert_eq!(
            sums(&d, kind).await,
            before,
            "{kind:?}: the readers saw nothing of the killed chunk"
        );
        one_chunk(&d).await;
        assert_eq!(
            staged(&d, kind).await.len(),
            CHUNK,
            "{kind:?}: the next chunk staged the same rows"
        );
        assert_exact(&mut d, kind, "a killed chunk").await;
    }
}

/// A page with a `b` change on key 20 and, committed after it, an `a` change
/// on that key's one row: the page writes the row from its `a` change, which
/// already joins the new `b` row, so its inline fan-out leaves the row as it
/// is and records nothing. The row's staged change carries the `b` change's
/// origin too: a token taken between the two commits waits for the readers.
#[tokio::test]
async fn an_a_rows_staged_change_carries_the_pages_b_origin() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        write(&d, "update public.b set w = 250, tag = 'late' where id = 2").await;
        let between = token(&d).await;
        write(&d, "update public.a set x = 80 where id = 8").await;
        let batch = d.seal().await;
        d.drain(batch, "page").await;
        assert_eq!(
            staged(&d, kind).await.len(),
            1,
            "{kind:?}: the page staged row 8 once"
        );
        assert!(
            !converged(&d, between).await,
            "{kind:?}: a token after the b change waits for the readers' row 8"
        );
        assert_exact(&mut d, kind, "an a change after a b change").await;
    }
}

/// `b` rows going and coming on a chunked key: an `INNER` target loses the
/// key's rows and gains them back, a `LEFT` one rewrites them, chunk by
/// chunk, and the readers apply each delete and insert from its images.
#[tokio::test]
async fn b_rows_going_and_coming_reach_the_readers() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        step(&mut d, "delete from public.b where id = 1").await;
        one_chunk(&d).await;
        drain_ring(&mut d).await;
        assert_eq!(
            sums(&d, kind).await,
            sums_of_source(&d, kind).await,
            "{kind:?}: mid fan-out of the delete"
        );
        assert_exact(&mut d, kind, "a b delete").await;
        step(&mut d, "insert into public.b values (3, 10, 300, 'back')").await;
        one_chunk(&d).await;
        drain_ring(&mut d).await;
        assert_eq!(
            sums(&d, kind).await,
            sums_of_source(&d, kind).await,
            "{kind:?}: mid fan-out of the insert"
        );
        assert_exact(&mut d, kind, "a b insert").await;
    }
}

/// A chunk whose rows include one a field fails on: the chunk's write rolls
/// back to its savepoint, the bisection writes the rest and holds the row,
/// and the readers get the rows the chunk wrote, each once, from the image
/// the rollback restored. The held row stays as it was, in the target and in
/// the readers.
#[tokio::test]
async fn a_chunk_holding_a_row_stages_the_rest() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        step(&mut d, "update public.a set x = 2147483000 where id = 3").await;
        assert_exact(&mut d, kind, "a large x").await;
        step(
            &mut d,
            "update public.b set w = 1000, tag = 'big' where id = 1",
        )
        .await;
        while !markers(&d, kind).await.is_empty() {
            one_chunk(&d).await;
            drain_ring(&mut d).await;
        }
        // The held row keeps the pipeline from quiescing, so drain what the
        // ring holds rather than settle.
        drain_ring(&mut d).await;
        let held: Vec<String> = d
            .rows(&format!(
                "select id, tag from {} where id = 3",
                kind.target()
            ))
            .await;
        assert_eq!(held, ["(3,ten)"], "{kind:?}: row 3 is held as it was");
        assert_eq!(
            sums(&d, kind).await,
            sums_of_source(&d, kind).await,
            "{kind:?}: the aggregate reader (left) differs from its source (right)"
        );
        let copy = d
            .rows(&format!(
                "select id, less, tag from {} order by id",
                kind.copy()
            ))
            .await;
        let copy_source = d
            .rows(&format!(
                "select id, total - 1, tag from {} order by id",
                kind.target()
            ))
            .await;
        assert_eq!(copy, copy_source, "{kind:?}: the 1-1 reader");
    }
}

/// A second `b` change on the key resets the cursor while the readers' rows
/// of the first change's chunks are half staged: the first chunk's drained,
/// the second's still in the ring. The chunks then rewrite every row from
/// the key's start, each recording the image it replaces, so the aggregate
/// reader's deltas telescope to the second change.
#[tokio::test]
async fn a_second_b_change_resets_the_cursor_with_the_readers_rows_half_staged() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        step(
            &mut d,
            "update public.b set w = 900, tag = 'mid' where id = 1",
        )
        .await;
        one_chunk(&d).await;
        drain_ring(&mut d).await;
        one_chunk(&d).await;
        assert_eq!(markers(&d, kind).await, ["({10},1,4)"], "{kind:?}");
        assert_eq!(
            staged(&d, kind).await.len(),
            CHUNK,
            "{kind:?}: the second chunk's rows wait in the ring"
        );
        step(
            &mut d,
            "update public.b set w = 555, tag = 'last' where id = 1",
        )
        .await;
        assert_eq!(
            markers(&d, kind).await,
            ["({10},2,)"],
            "{kind:?}: the second change reset the cursor"
        );
        assert_exact(&mut d, kind, "two b changes").await;
    }
}

// ------------------------------------- the build's writers (#1617 step B)

/// Runs `sql` on `table` past capture: its triggers off for the write, so no
/// change reaches the ring and only a rebuild or a Re-derive finds it.
async fn past_capture(d: &Driver, table: &str, sql: &str) {
    let toggle = |state: &str| {
        format!(
            "do $$ declare t record; begin \
                 for t in select tgname from pg_trigger \
                          where tgrelid = '{table}'::regclass and not tgisinternal loop \
                     execute format('alter table {table} {state} trigger %I', t.tgname); \
                 end loop; \
             end $$;"
        )
    };
    d.ctl
        .batch_execute(&format!(
            "{} {sql}; {}",
            toggle("disable"),
            toggle("enable always")
        ))
        .await
        .expect(sql);
}

async fn trellis(d: &Driver) -> trellis::Trellis {
    trellis::Trellis::connect(
        trellis::config::Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

/// Runs every build to its end, two rows to a chunk and sweep batch.
async fn run_builds(d: &Driver) {
    trellis::staging::build::settle_builds_with(d.pool(), 2).await;
}

/// The definition's status.
async fn status(d: &Driver, kind: Kind) -> String {
    d.ctl
        .query_one(
            "select status::text from transform_definitions where target_table = $1",
            &[&kind.target()],
        )
        .await
        .expect("the definition")
        .get(0)
}

/// A rebuild in place (`request_backfill`) after writes capture never saw:
/// the build's chunks rewrite the rows of the `a` and `b` rows that changed,
/// and the sweep's stale pass deletes the row of the `a` row that went. Each
/// stages its rows for the readers in its own transaction.
#[tokio::test]
async fn a_rebuild_reaches_the_readers() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        past_capture(&d, "public.a", "update public.a set x = 50 where id = 1").await;
        past_capture(
            &d,
            "public.b",
            "update public.b set w = 150, tag = 'quiet' where id = 2",
        )
        .await;
        past_capture(&d, "public.a", "delete from public.a where id = 5").await;
        let trellis = trellis(&d).await;
        trellis
            .request_backfill("public.b")
            .await
            .expect("request a rebuild");
        assert_eq!(status(&d, kind).await, "backfilling", "{kind:?}");
        // The 1-1 JOIN over the target reads `a` too, and capture missed
        // `a`'s delete for it as well.
        trellis
            .request_backfill("public.a")
            .await
            .expect("request a rebuild of a's readers");
        let token = token(&d).await;
        run_builds(&d).await;
        assert!(
            !converged(&d, token).await,
            "{kind:?}: the rebuild's staged rows hold the token back"
        );
        assert_exact(&mut d, kind, "a rebuild").await;
    }
}

/// A `TRUNCATE` of `a` empties the target through the page, and the readers
/// get every row's delete; rows inserted after it reach them as before.
#[tokio::test]
async fn a_truncate_of_a_reaches_the_readers() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        step(&mut d, "truncate public.a").await;
        assert!(
            d.rows(&format!("select id from {}", kind.target()))
                .await
                .is_empty(),
            "{kind:?}: the truncate emptied the target"
        );
        assert_exact(&mut d, kind, "a truncate of a").await;
        assert!(sums(&d, kind).await.is_empty(), "{kind:?}");
        step(
            &mut d,
            "insert into public.a values (1, 10, 1, 'one'), (8, 20, 8, 'eight')",
        )
        .await;
        assert_exact(&mut d, kind, "inserts after a truncate of a").await;
    }
}

/// A `TRUNCATE` of `b`, and a `b` row inserted after it, start a rebuild of
/// the target: its chunks rewrite every row (a `LEFT` row loses its `b`
/// columns, an `INNER` row goes, key 10's rows join the new row) and stage
/// them for the readers.
#[tokio::test]
async fn a_truncate_of_b_rebuild_reaches_the_readers() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        step(
            &mut d,
            "truncate public.b; insert into public.b values (3, 10, 555, 'again')",
        )
        .await;
        assert_eq!(
            status(&d, kind).await,
            "backfilling",
            "{kind:?}: the truncate started a rebuild"
        );
        run_builds(&d).await;
        assert_exact(&mut d, kind, "a truncate of b").await;
    }
}

/// Re-derives a producer stages (`Recompute` rows, as a backfill, a
/// catch-up or a column resume stage them) after writes capture never saw:
/// an `a` row's page rewrites its row, and a `b` row's page fans out to its
/// key's row; both reach the readers.
#[tokio::test]
async fn a_rederive_reaches_the_readers() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        past_capture(&d, "public.a", "update public.a set x = 70 where id = 9").await;
        past_capture(&d, "public.a", "update public.a set x = 30 where id = 3").await;
        past_capture(
            &d,
            "public.b",
            "update public.b set w = 260, tag = 'redone' where id = 2",
        )
        .await;
        d.stage_recomputes("public.a", &["3", "9"]).await;
        d.stage_recomputes("public.b", &["2"]).await;
        drain_ring(&mut d).await;
        assert_exact(&mut d, kind, "Re-derives").await;
    }
}

/// A `RESUME` recreates `R` and `M` empty and keeps the target, so its
/// rebuild's chunks rewrite the rows of the changes the pause missed, and
/// the sweep's orphan pass deletes the row of an `a` row deleted while
/// paused. Each stages its rows for the readers, which stay live through the
/// pause.
#[tokio::test]
async fn a_resume_rebuild_reaches_the_readers() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        let trellis = trellis(&d).await;
        trellis
            .apply(&format!("PAUSE TRANSFORM {}", kind.name()))
            .await
            .expect("pause");
        step(
            &mut d,
            "update public.a set x = 60 where id = 2; delete from public.a where id = 8; \
             update public.b set w = 110, tag = 'woke' where id = 1",
        )
        .await;
        trellis
            .apply(&format!("RESUME TRANSFORM {}", kind.name()))
            .await
            .expect("resume");
        trellis::intake::harness::start_registered_builds(d.pool())
            .await
            .expect("start the resumed build");
        assert_eq!(status(&d, kind).await, "backfilling", "{kind:?}");
        run_builds(&d).await;
        assert_exact(&mut d, kind, "a resume").await;
    }
}

/// A row a page held (its field overflows) is released once its cause is
/// gone: the release re-derives it in its own transaction and stages its row
/// for the readers, before the `Recompute` it stages finds the row written.
#[tokio::test]
async fn a_quarantine_release_reaches_the_readers() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        step(&mut d, "update public.a set x = 2147483600 where id = 8").await;
        assert_eq!(
            d.rows("select key from poison").await,
            ["(8)"],
            "{kind:?}: the page held row 8"
        );
        step(&mut d, "update public.a set x = 18 where id = 8").await;
        let held =
            trellis::staging::quarantine::release_key(d.pool(), kind.name(), "public.a", "8")
                .await
                .expect("release the key");
        assert_eq!(held, 1, "{kind:?}: row 8 had its parked work");
        assert_exact(&mut d, kind, "a release").await;
        assert!(
            d.rows("select key from poison").await.is_empty(),
            "{kind:?}"
        );
    }
}

/// A reader's own field failing on a row the 1-1 `JOIN` wrote (`total * 2`
/// overflows `int4`) holds that key for that reader alone, as for a reader
/// of any target: each failed drain charges the key, the drain at the death
/// threshold evicts it and commits the rest, and the other readers apply the
/// row. Once the cause is gone, a release brings the reader back to its
/// source.
#[tokio::test]
async fn a_readers_failing_field_holds_the_key_for_that_reader_alone() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        let double = format!("{}_double", kind.name());
        let columns: HashMap<String, ValueType> = [("total".to_string(), int4())].into();
        trellis::defs::install_definition(
            d.pool(),
            &format!(
                "TRANSFORM {double} FROM {} SELECT total * 2 AS twice",
                kind.target()
            ),
            &columns,
            "public",
        )
        .await
        .expect("define the doubling reader");
        trellis::intake::harness::settle_builds(d.pool()).await;
        d.settle().await;

        // The 1-1 JOIN writes row 8 (`total` fits), and stages it.
        step(&mut d, "update public.a set x = 1500000000 where id = 8").await;
        let batch = d.seal().await;
        let mut failures = 0;
        loop {
            match trellis::staging::apply::drain_once(
                d.pool(),
                batch,
                "readers",
                1,
                drain_driver::WAKE,
            )
            .await
            {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(err) => {
                    assert!(
                        err.to_string().contains("integer out of range"),
                        "{kind:?}: {err}"
                    );
                    failures += 1;
                    assert!(failures < 20, "{kind:?}: the batch never drained");
                }
            }
        }
        assert_eq!(
            failures,
            trellis::staging::DEFAULT_DEATH_THRESHOLD as usize - 1,
            "{kind:?}: the drains before the threshold surface the failure, the one at it \
             evicts the key"
        );
        let double_id: i64 = d
            .ctl
            .query_one(
                "select id from transform_definitions where target_table = $1",
                &[&format!("public.{double}")],
            )
            .await
            .expect("the doubling reader")
            .get(0);
        assert_eq!(
            d.rows("select transform_id, src_table, key from poison")
                .await,
            [format!("({double_id},{},8)", kind.target())],
            "{kind:?}: key 8 is held for the doubling reader alone"
        );
        let copy = d
            .rows(&format!("select less from {} where id = 8", kind.copy()))
            .await;
        assert_eq!(
            copy,
            ["(1500000199)"],
            "{kind:?}: the 1-1 reader applied row 8"
        );

        // The cause goes; the release re-derives the key for the reader.
        step(&mut d, "update public.a set x = 8 where id = 8").await;
        drain_ring(&mut d).await;
        let held =
            trellis::staging::quarantine::release_key(d.pool(), &double, &kind.target(), "8")
                .await
                .expect("release the key");
        assert_eq!(held, 1, "{kind:?}: row 8 had its parked work");
        assert_exact(&mut d, kind, "a release of the reader's key").await;
        assert_eq!(
            d.rows(&format!(
                "select id, twice from public.{double} order by id"
            ))
            .await,
            d.rows(&format!(
                "select id, total * 2 from {} order by id",
                kind.target()
            ))
            .await,
            "{kind:?}: the doubling reader equals its source"
        );
        assert_self_check_clean(&d, &format!("public.{double}"), "the release").await;
    }
}

/// Every definition's status, in definition order.
async fn statuses(d: &Driver) -> Vec<String> {
    d.rows("select status::text from transform_definitions order by id")
        .await
}

/// Runs the staging worker's capture pass twice.
async fn capture_passes(d: &mut Driver) {
    let pool = d.pool().clone();
    for _ in 0..2 {
        trellis::client::reconcile_pass(&mut d.ctl, &pool, trellis::config::DEFAULT_SCHEMA)
            .await
            .expect("a capture pass");
    }
}

/// A widening of a column the 1-1 `JOIN` copies (`a.x`, into `total`) pauses
/// it. Its `RESUME` re-types the target's column, which pauses the readers
/// whose own columns it outgrew, and each resumes on its own once its
/// upstream is live, down the chain; then a value only the wider type holds
/// reaches every reader. The widening rewrites `a`, and PostgreSQL's 2026-11
/// minors re-version `y`, after `x`, so the reader of `a.y` rebuilds
/// (`support/attver.rs`).
#[tokio::test]
async fn a_widening_resumes_down_the_readers() {
    let _chunked = chunked().await;
    for kind in KINDS {
        let mut d = start(kind).await;
        let widen = "alter table public.a alter column x type bigint";
        d.ctl.batch_execute(widen).await.expect("widen a.x");
        attver::Attver::probe(&d.ctl)
            .await
            .after(&d.ctl, widen)
            .await;
        capture_passes(&mut d).await;
        assert_eq!(
            statuses(&d).await,
            ["(paused)", "(live)", "(live)", "(live)", "(backfilling)"],
            "{kind:?}: the widening paused the 1-1 JOIN"
        );
        trellis(&d)
            .await
            .apply(&format!("RESUME TRANSFORM {}", kind.name()))
            .await
            .expect("resume");
        for _ in 0..3 {
            capture_passes(&mut d).await;
            trellis::intake::harness::start_registered_builds(d.pool())
                .await
                .expect("start the resumed builds");
            run_builds(&d).await;
            d.settle().await;
        }
        assert_eq!(
            statuses(&d).await,
            ["(live)"; 5],
            "{kind:?}: every reader resumed"
        );
        step(&mut d, "update public.a set x = 3000000000 where id = 1").await;
        assert_exact(&mut d, kind, "a widening").await;
    }
}
