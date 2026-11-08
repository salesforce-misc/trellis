//! The star 1-1 `JOIN` fixture (`support/star.rs`, #1464 step 2a) and a few
//! representative two-table pins re-expressed through it.
//!
//! - The fixture's generated text: the two-table shape's definition and
//!   oracle, and the DDL, seed and oracle of every shape up to the cap, run on
//!   a plain Postgres database with no transform defined.
//! - Pins written once against "the joined table at `at`" and run at every
//!   shape this process can define ([`star::runnable`]): the two-table JOIN
//!   always, a star of three tables (its last position) only with the
//!   development knob on.
//!   - the build, then changes of `a` and of the table at `at`;
//!   - F3 (#1371): an `a` move racing an update of its new key, a family;
//!   - M (#1375): two crossing mixed pages, a race;
//!   - T1 and T2 (#1375): a truncate of either side racing a page of the
//!     other, races whose truncate may start a rebuild;
//!   - C (#1375): a page on a build chunk's key queues behind the chunk;
//!   - the chunked fan-out (#1405): a change past one chunk finishes in
//!     chunks.
//!
//! The two-table suites (`join_one_to_one_*.rs`) keep their own fixtures;
//! the same bodies run here at `Shape { positions: 1, at: 1 }`.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use star::{Kind, Shape, Star};
use trellis::dev::defs::set_one_to_one_join_fan_out_chunk_rows;
use trellis::staging::OneToOneJoinFanOutStep;
use trellis::staging::build::Step;
use trellis::staging::interleave::PausePoint;

/// The shapes the per-position pins run at: the two-table JOIN, and the last
/// position of a three-table star.
const SHAPES: [Shape; 2] = [
    Shape::TWO,
    Shape {
        positions: 2,
        at: 2,
    },
];

/// [`SHAPES`] this process can define; a skipped shape says so.
fn shapes() -> Vec<Shape> {
    SHAPES
        .into_iter()
        .filter(|shape| {
            let runs = star::runnable(*shape);
            if !runs {
                eprintln!("{shape:?} skipped: {} is off", star::STAR_KNOB);
            }
            runs
        })
        .collect()
}

const A_ROWS: &str =
    "(1, 10, 1, 'one'), (2, 20, 2, 'two'), (3, 30, 3, 'three'), (4, null, 4, 'four')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty'), (3, 40, 400, 'forty')";

// ------------------------------------------------------- the generated text

/// At one position the fixture's definition is the two-table suites' text,
/// with the joined table and the key column numbered, and its oracle the
/// live join.
#[test]
fn the_two_table_shape_renders_the_two_table_join() {
    let [left, inner] = Shape::TWO.stars()[..] else {
        panic!("two kinds at one position")
    };
    assert_eq!(left.name(), "jl");
    assert_eq!(inner.target(), "public.ji");
    assert_eq!(
        left.definition(),
        "TRANSFORM jl FROM public.a LEFT JOIN public.b1 ON a.k1 = b1.k \
         SELECT a.x + COALESCE(b1.w, 0) AS total, b1.tag AS tag1, a.y AS y"
    );
    assert_eq!(
        inner.expected(),
        "select a.id, a.x + coalesce(b1.w, 0) as total, b1.tag, a.y \
         from public.a a join public.b1 b1 on a.k1 = b1.k"
    );
    assert_eq!(inner.actual(), "select id, total, tag1, y from public.ji");
    assert_eq!(
        Shape::TWO.sql("insert into {a_ins} values (5, 30, 5, 'five'); update {b} set w = 1"),
        "insert into public.a (id, k1, x, y) values (5, 30, 5, 'five'); \
         update public.b1 set w = 1"
    );
}

/// The kind tuples: both kinds at one position, the 4 pairs at two and the
/// strength-2 cover at three, which holds every ordered pair of kinds at
/// every pair of positions.
#[test]
fn the_kind_tuples_cover_every_pair_of_positions() {
    assert_eq!(Shape::new(1, 1).tuples().len(), 2);
    for positions in 2..=star::MAX_POSITIONS {
        let tuples = Shape::new(positions, 1).tuples();
        for p in 0..positions {
            for q in p + 1..positions {
                for pair in [
                    [Kind::Left, Kind::Left],
                    [Kind::Left, Kind::Inner],
                    [Kind::Inner, Kind::Left],
                    [Kind::Inner, Kind::Inner],
                ] {
                    assert!(
                        tuples.iter().any(|t| [t[p], t[q]] == pair),
                        "{positions} positions: no tuple has {pair:?} at ({p}, {q})"
                    );
                }
            }
        }
        let names: std::collections::HashSet<String> = Shape::new(positions, 1)
            .stars()
            .iter()
            .map(|s| s.name())
            .collect();
        assert_eq!(names.len(), tuples.len(), "one name per tuple");
    }
}

/// A three-table star's definition names each position's kind, table and
/// key, and the bodies' placeholders fill for the position under test.
#[test]
fn a_star_renders_every_position() {
    let shape = Shape::new(2, 2);
    let star = Star {
        shape,
        kinds: &[Kind::Inner, Kind::Left],
    };
    assert_eq!(star.name(), "jil");
    assert_eq!(
        star.definition(),
        "TRANSFORM jil FROM public.a INNER JOIN public.b1 ON a.k1 = b1.k \
         LEFT JOIN public.b2 ON a.k2 = b2.k \
         SELECT a.x + COALESCE(b1.w, 0) + COALESCE(b2.w, 0) AS total, \
         b1.tag AS tag1, b2.tag AS tag2, a.y AS y"
    );
    assert_eq!(
        shape.sql("update public.a set {k} = 20 where id = 1; delete from {b}"),
        "update public.a set k2 = 20 where id = 1; delete from public.b2"
    );
    assert_eq!(shape.idle().collect::<Vec<_>>(), [1]);
    assert_eq!(shape.idle_total(), 1000);
}

/// Every shape's DDL and seed run on Postgres, and its oracle reads them:
/// the idle positions match every seeded and inserted `a` row on keys of
/// their own, so each kind tuple keeps exactly the rows the position under
/// test keeps (all four `a` rows where it is `LEFT`, the two matched ones
/// where it is `INNER`). No transform is defined: this checks the fixture,
/// not the engine.
#[tokio::test]
async fn every_shapes_tables_and_oracle_run_on_postgres() {
    let cluster = testkit::TestCluster::start();
    for positions in 1..=star::MAX_POSITIONS {
        for at in [1, positions] {
            let shape = Shape::new(positions, at);
            let db = cluster.create_isolated_database().await;
            let client = drain_driver::connect(db.dsn()).await;
            client
                .batch_execute(&format!(
                    "{}; {}; {}",
                    shape.ddl(),
                    shape.seed(A_ROWS, B_ROWS),
                    shape.sql("insert into {a_ins} values (5, 10, 5, 'five')")
                ))
                .await
                .unwrap_or_else(|e| panic!("{shape:?}: {e}"));
            for star in shape.stars() {
                let rows = client
                    .query(&star.expected(), &[])
                    .await
                    .unwrap_or_else(|e| panic!("{}: {e}", star.name()));
                let want = match star.kind(at) {
                    Kind::Left => 5,
                    Kind::Inner => 3,
                };
                assert_eq!(rows.len(), want, "{shape:?} {}", star.name());
                for row in rows.iter().filter(|r| r.get::<_, i32>(0) == 1) {
                    assert_eq!(
                        row.get::<_, i32>(1),
                        1 + 100 + shape.idle_total(),
                        "{shape:?} {}: row 1's total",
                        star.name()
                    );
                }
            }
        }
    }
}

// --------------------------------------------------------------------- pins

/// The build fills the target, `R` and every `M` from the live tables, and
/// changes of `a` and of the table at `at` apply: an insert, a non-key
/// update, key moves onto and off a match and to `NULL`, a delete and a
/// re-insert.
#[tokio::test]
async fn the_build_and_changes_of_either_side_apply() {
    let _serial = star::serial().await;
    for shape in shapes() {
        let stars = shape.stars();
        let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
        star::assert_oracle(&mut d, &stars).await;
        for sql in [
            "insert into {a_ins} values (5, 10, 5, 'five'), (6, 99, 6, 'six'), \
             (7, null, 7, 'seven')",
            "update public.a set x = x + 10 where id = 1",
            "update public.a set {k} = 20 where id = 3",
            "update public.a set {k} = null where id = 1",
            "delete from public.a where id in (2, 5)",
            "insert into {a_ins} values (2, 10, 22, 'again')",
            "insert into {b} values (4, 30, 300, 'thirty')",
            "update {b} set w = 111, tag = 'TEN' where id = 1",
            "update {b} set k = 99 where id = 2",
            "update {b} set k = null where id = 4",
            "delete from {b} where id = 1",
        ] {
            star::step(&mut d, &shape.sql(sql)).await;
            star::assert_oracle(&mut d, &stars).await;
        }
    }
}

/// F3 (#1371): an `a` key move `10 → 20` ‖ an update of key 20 in the table
/// at `at`; whichever drains first, frozen at its commit, the other queues
/// on `J[20]`.
#[tokio::test]
async fn f3_an_a_move_races_a_b_update_of_its_new_key() {
    let _serial = star::serial().await;
    for shape in shapes() {
        star::family(
            shape,
            A_ROWS,
            B_ROWS,
            "update public.a set {k} = 20 where id = 1",
            "update {b} set w = 222 where id = 2",
        )
        .await;
    }
}

/// Two crossing mixed pages (#1375's M1–M6): one moves `a` row 1 from 10 to
/// 20 and changes key 20, the other moves row 2 (or row 3) onto 10 and
/// changes key 10. Each locks both its keys in one sorted `J` statement, so
/// the second queues on the first and neither deadlocks, in either order.
#[tokio::test]
async fn m_two_crossing_mixed_pages_queue_on_j_without_deadlock() {
    let _serial = star::serial().await;
    for shape in shapes() {
        for other_a in [
            "update public.a set {k} = 10 where id = 2",
            "update public.a set {k} = 10 where id = 3",
        ] {
            for star in shape.stars() {
                for one_first in [true, false] {
                    let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
                    star::write(&d, &shape.sql("update public.a set {k} = 20 where id = 1")).await;
                    star::write(&d, &shape.sql("update {b} set w = 250 where id = 2")).await;
                    let one = d.seal().await;
                    star::write(&d, &shape.sql(other_a)).await;
                    star::write(&d, &shape.sql("update {b} set w = 150 where id = 1")).await;
                    let two = d.seal().await;
                    let (first, second) = if one_first { (one, two) } else { (two, one) };
                    star::race(&mut d, star, first, second).await;
                }
            }
        }
    }
}

/// T1 and T2 (#1375): a `TRUNCATE` of `a` ‖ an update of the table at `at`,
/// and a `TRUNCATE` of that table ‖ an `a` insert on a key it held, each in
/// either order. The second page queues on the pair's lock row, which the
/// truncate's page holds `FOR UPDATE` and every other page `FOR SHARE`, and
/// the rebuild a truncate of the joined table starts is exact.
#[tokio::test]
async fn t1_t2_a_truncate_races_a_page_of_the_other_side() {
    let _serial = star::serial().await;
    for shape in shapes() {
        for (name, x, y) in [
            (
                "t1 truncate a vs b",
                "truncate public.a",
                "update {b} set w = 999 where id = 2",
            ),
            (
                "t2 truncate b vs a",
                "truncate {b}",
                "insert into {a_ins} values (5, 20, 5, 'five')",
            ),
        ] {
            for star in shape.stars() {
                for x_first in [true, false] {
                    let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
                    star::write(&d, &shape.sql(x)).await;
                    let sealed_x = d.seal().await;
                    star::write(&d, &shape.sql(y)).await;
                    let sealed_y = d.seal().await;
                    let (first, second) = if x_first {
                        (sealed_x, sealed_y)
                    } else {
                        (sealed_y, sealed_x)
                    };
                    star::race_rebuilt(&mut d, star, name, first, second).await;
                }
            }
        }
    }
}

/// A build chunk frozen with everything written holds its `J` rows, so a
/// page of the table at `at` on one of its keys queues behind it and reads
/// `R` and the target only once the chunk commits (#1375's C families: a
/// chunk first).
#[tokio::test]
async fn c_a_page_on_a_chunks_key_queues_behind_the_chunk() {
    let _serial = star::serial().await;
    for shape in shapes() {
        for star in shape.stars() {
            let mut d = star::start_unbuilt(&[star], A_ROWS, B_ROWS).await;
            assert_eq!(
                star::build_step(d.pool(), "planner", 100).await,
                Step::Planned
            );
            let target = star.target();
            let mut chunk = d
                .run_frozen(
                    &[(PausePoint::BeforeRebuildCommit, &target)],
                    |pool| async move { Ok(star::build_step(&pool, "chunk", 100).await) },
                )
                .await;
            let reached = chunk.reached(PausePoint::BeforeRebuildCommit).await;
            star::write(&d, &shape.sql("update {b} set w = 777 where id = 1")).await;
            let batch = d.seal().await;
            let page = d.drain_frozen(batch, "page", &[]).await;
            d.wait_blocked_behind(reached.backend_pid).await;
            assert!(
                !page.is_finished(),
                "{}: the page queued on the chunk's J row",
                star.name()
            );
            d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
            chunk.finish().await;
            page.finish().await;
            trellis::staging::build::settle_builds_with(d.pool(), 3).await;
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// The fan-out chunk these pins run with.
const CHUNK: usize = 2;

/// Puts the process-wide fan-out chunk back when a pin ends, even on a
/// panic.
struct Chunked;

impl Drop for Chunked {
    fn drop(&mut self) {
        set_one_to_one_join_fan_out_chunk_rows(0);
    }
}

/// `star`'s fan-out markers, as `(join key, gen, cursor)`.
async fn markers(d: &drain_driver::Driver, star: Star) -> Vec<String> {
    d.rows(&format!(
        "select p.join_key, p.gen, p.cursor from join1_fan_out p \
         join transform_definitions d on d.id = p.transform_id \
         where d.target_table = '{}' order by p.join_key",
        star.target()
    ))
    .await
}

/// `star`'s target rows whose `a` row is on key 10 at `at`, as
/// `(id, total, tag)` with the tag of the table at `at`.
async fn key_rows(d: &drain_driver::Driver, star: Star) -> Vec<String> {
    let shape = star.shape;
    d.rows(&format!(
        "select t.id, t.total, t.tag{at} from {} t join public.a a using (id) \
         where a.{k} = 10 order by t.id",
        star.target(),
        at = shape.at,
        k = shape.k()
    ))
    .await
}

/// `(id, total, tag)` for ids `ids` on key 10, joined to `w` and `tag` at
/// `at` and to the idle positions.
fn joined(shape: Shape, ids: std::ops::RangeInclusive<i32>, w: i32, tag: &str) -> Vec<String> {
    ids.map(|id| format!("({id},{},{tag})", id + w + shape.idle_total()))
        .collect()
}

async fn one_chunk(d: &drain_driver::Driver) {
    let step = trellis::staging::one_to_one_join_fan_out_step(d.pool())
        .await
        .expect("a chunk");
    assert_eq!(step, OneToOneJoinFanOutStep::Chunk);
}

/// A change of the table at `at` on a key with more `a` rows than one
/// chunk (#1405): its page commits `M` and a marker at the key's start and
/// writes none of the key's rows, and each chunk recomputes the next two rows
/// in key order and advances the cursor, until the last deletes the marker.
#[tokio::test]
async fn a_b_change_past_one_chunk_finishes_in_chunks() {
    let _serial = star::serial().await;
    set_one_to_one_join_fan_out_chunk_rows(CHUNK);
    let _chunked = Chunked;
    let a_rows = "(1, 10, 1, 'one'), (2, 10, 2, 'two'), (3, 10, 3, 'three'), \
                  (4, 10, 4, 'four'), (5, 10, 5, 'five'), (6, 10, 6, 'six'), \
                  (7, 10, 7, 'seven'), (8, 20, 8, 'eight'), (9, null, 9, 'nine')";
    let b_rows = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty')";
    for shape in shapes() {
        for star in shape.stars() {
            let name = star.name();
            let mut d = star::start(&[star], a_rows, b_rows).await;
            star::step(
                &mut d,
                &shape.sql("update {b} set w = 900, tag = 'new' where id = 1"),
            )
            .await;
            assert_eq!(markers(&d, star).await, ["({10},1,)"], "{name}");
            assert_eq!(
                key_rows(&d, star).await,
                joined(shape, 1..=7, 100, "ten"),
                "{name}: the page wrote none of the key's rows"
            );
            one_chunk(&d).await;
            assert_eq!(markers(&d, star).await, ["({10},1,2)"], "{name}");
            let mut rows = joined(shape, 1..=2, 900, "new");
            rows.extend(joined(shape, 3..=7, 100, "ten"));
            assert_eq!(key_rows(&d, star).await, rows, "{name}");
            for _ in 0..3 {
                one_chunk(&d).await;
            }
            assert!(markers(&d, star).await.is_empty(), "{name}: the last chunk");
            assert_eq!(
                key_rows(&d, star).await,
                joined(shape, 1..=7, 900, "new"),
                "{name}"
            );
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}
