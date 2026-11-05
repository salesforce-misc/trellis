//! Readers of a 1-1 `JOIN` target across a chunked fan-out (#1617, step A of
//! its rule 4): a page's writes of its `a` changes' rows and of its inline
//! fan-out, and each fan-out chunk, stage the target's changes for its
//! readers in their own transaction, a chunk's with its marker's origin.
//!
//! Define refuses a reader of a 1-1 `JOIN` target until every writer of one
//! stages for readers, so these tests admit readers through the test-only
//! switch (`admit_readers_for_tests`). The tables start empty and every row
//! arrives through pages, so the only writers are the ones this step wires:
//! the build writes nothing.
//!
//! Two readers read the target: an aggregate on the group basis, which
//! applies each change's images as a delta, and a 1-1 transform. The chunk
//! is shrunk to two rows over a key of seven `a` rows, and pages and chunks
//! are driven by hand (`support/drain_driver.rs`), frozen at a
//! [`PausePoint`] where a scenario needs it. Nothing sleeps or polls for
//! convergence. Each scenario runs on both join kinds.
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
//! each ending with every reader equal to its source and the token taken
//! after the scenario's writes converged.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::dev::defs::set_one_to_one_join_fan_out_chunk_rows;
use trellis::integer::IntWidth;
use trellis::staging::OneToOneJoinFanOutStep;
use trellis::staging::interleave::PausePoint;

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
    trellis::defs::join_one_to_one::admit_readers_for_tests();
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
}

/// `kind` and its two readers over empty tables, built and live, `a` and
/// `b` captured; then `a`'s rows and `b`'s, each a page of its own, and
/// everything settled. The `b` page fans key 20 out itself and leaves key 10
/// to chunks.
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
            "TRANSFORM {}_copy FROM {} SELECT total * 2 AS twice, tag AS tag",
            kind.name(),
            kind.target()
        ),
    ] {
        trellis::defs::install_definition(d.pool(), &reader, &columns, "public")
            .await
            .expect("define a reader");
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
            "select id, twice, tag from {} order by id",
            kind.copy()
        ))
        .await;
    let copy_source = d
        .rows(&format!(
            "select id, total * 2, tag from {} order by id",
            kind.target()
        ))
        .await;
    assert_eq!(
        copy, copy_source,
        "{kind:?}: the 1-1 reader (left) differs from its source (right) after {after}"
    );
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
