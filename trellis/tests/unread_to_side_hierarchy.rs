//! A relationship's to-side that no definition reads through, attached to a
//! hierarchy and detached again while nothing reads it (issue #909).
//!
//! The staging worker's capture pass leaves a table in a partition or
//! inheritance hierarchy alone and pauses the definitions that read it
//! (#707, case 3). A relationship's to-side is captured whenever some
//! definition reads the relationship's from-side, so a to-side nothing reads
//! *through* is skipped with nobody to pause, and writes routed through its
//! parent are never captured. Its settled projection stops advancing. This
//! pins that the stale projection is never served: the first definition to
//! read through the relationship re-derives the projection from the to-side
//! (`catalog::relationships_to_refresh`, #768) before it goes live.
//!
//! Nothing polls for convergence (#297): every pass is stepped by hand.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::{Config, Trellis, TrellisOptions};

/// `public.par (id, w, x)` with parents 1 and 2, `public.src (id, g, p)` with
/// children 1 and 2 of parent 1 and 3 of parent 2, the relationship `parent`
/// from `src.p` to `par.id`, and one definition, `sg`, on `src` that reads no
/// relationship. `par` is captured (it is `src`'s relationship's to-side) and
/// nothing reads through the relationship.
///
/// With `dropped_reader`, a definition `old` first reads `parent.w` and is
/// dropped: the projection keeps the data column it widened, so the writes
/// the pass misses leave stale data and not only stale keys.
async fn start(dropped_reader: bool) -> Driver {
    let columns: Vec<(&str, ValueType)> = vec![
        ("id", ValueType::Numeric),
        ("g", ValueType::Numeric),
        ("p", ValueType::Numeric),
        ("w", ValueType::Numeric),
    ];
    let d = Driver::start_with_relationships(
        "create table public.par (id integer primary key, w numeric, x numeric); \
         create table public.src (id integer primary key, g integer, p integer); \
         insert into public.par values (1, 10, 100), (2, 20, 200); \
         insert into public.src values (1, 1, 1), (2, 1, 1), (3, 2, 2), (4, 2, 3);",
        &columns,
        &["RELATIONSHIP parent FROM src.p TO par.id"],
        &["TRANSFORM sg FROM public.src SELECT g AS g"],
        &["public.par", "public.src"],
    )
    .await;
    if dropped_reader {
        let mut d = d;
        define(
            &mut d,
            "TRANSFORM old FROM public.src SELECT parent.w AS pw",
        )
        .await;
        let trellis = Trellis::connect(
            Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
            TrellisOptions::default(),
        )
        .await
        .expect("connect");
        trellis.apply("PAUSE TRANSFORM old").await.expect("pause");
        trellis.apply("DROP TRANSFORM old").await.expect("drop");
        trellis.shutdown().await.expect("shutdown");
        return d;
    }
    d
}

fn src_columns() -> std::collections::HashMap<String, ValueType> {
    [
        ("id", ValueType::Numeric),
        ("g", ValueType::Numeric),
        ("p", ValueType::Numeric),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

/// One capture pass over every table the catalog says to capture.
async fn capture_pass(d: &mut Driver) {
    let desired = trellis::defs::tables_to_capture(d.pool())
        .await
        .expect("the tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        &mut d.ctl,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn statuses(d: &Driver) -> Vec<String> {
    d.rows("select target_table, status from transform_definitions order by 1")
        .await
}

/// Defines `text` on `public.src` and settles it to `live`.
async fn define(d: &mut Driver, text: &str) {
    trellis::defs::install_definition(d.pool(), text, &src_columns(), "public")
        .await
        .expect("define");
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
}

/// Whether the projection no longer matches the to-side: its keys, and its
/// `w` column when it has one. True before the define proves the writes were
/// missed, so the scenario starts from a stale projection.
async fn stale_projection(d: &Driver) -> bool {
    let table: String = d
        .ctl
        .query_one(
            "select projection_table from relationship_projections p \
             join relationship_definitions r on r.id = p.relationship_id \
             where r.name = 'parent'",
            &[],
        )
        .await
        .expect("the relationship's projection")
        .get(0);
    let has_w: bool = d
        .ctl
        .query_one(
            "select exists (select 1 from information_schema.columns \
             where table_schema = $1 and table_name = $2 and column_name = 'w')",
            &[&DEFAULT_SCHEMA, &table],
        )
        .await
        .expect("read the projection's columns")
        .get(0);
    let columns = if has_w { "id, w" } else { "id" };
    let projected = d
        .rows(&format!(
            "select {columns} from \"{DEFAULT_SCHEMA}\".\"{table}\" order by id"
        ))
        .await;
    let actual = d
        .rows(&format!("select {columns} from public.par order by id"))
        .await;
    projected != actual
}

async fn assert_reader_matches_the_join(d: &Driver) {
    assert_eq!(
        d.rows("select id, g, pw, px from public.one order by id")
            .await,
        d.rows(
            "select s.id, s.g, p.w, p.x from public.src s left join public.par p on p.id = s.p \
             order by s.id"
        )
        .await,
        "one (left) differs from the join (right)"
    );
}

/// Defines the reader, after the to-side is plain again, and checks it
/// against the join.
async fn define_the_reader_and_compare(d: &mut Driver, expect_stale: bool) {
    assert_eq!(
        stale_projection(d).await,
        expect_stale,
        "the scenario's writes should leave the projection stale"
    );
    define(
        d,
        "TRANSFORM one FROM public.src SELECT g AS g, parent.w AS pw, parent.x AS px",
    )
    .await;
    assert_eq!(statuses(d).await, ["(public.one,live)", "(public.sg,live)"]);
    assert_reader_matches_the_join(d).await;

    // The build read the to-side itself. What the settled projection holds
    // is read by every later change of a from-side row: one pointing at a
    // parent the missed writes deleted, one at a parent they inserted, one
    // that only changes another column.
    d.ctl
        .batch_execute(
            "insert into public.src values (5, 2, 2), (6, 3, 3); \
             update public.src set g = 7 where id = 1; \
             update public.src set p = 2 where id = 4",
        )
        .await
        .expect("write the from-side");
    d.settle().await;
    assert_reader_matches_the_join(d).await;
}

/// Writes that never reach capture while `par` is a partition, then a
/// reader defined once it is plain again: the reader reads the to-side as it
/// is now, not the projection as it was.
async fn partition_scenario(dropped_reader: bool) {
    let mut d = start(dropped_reader).await;
    d.ctl
        .batch_execute(
            "create table public.par_all (id int not null, w numeric, x numeric) \
                 partition by range (id); \
             alter table public.par_all attach partition public.par \
                 for values from (minvalue) to (maxvalue)",
        )
        .await
        .expect("attach the to-side as a partition");
    capture_pass(&mut d).await;
    // Nothing reads through the relationship, so there is nobody to pause.
    assert_eq!(statuses(&d).await, ["(public.sg,live)"]);

    // Routed through the parent: a statement trigger on the partition never
    // sees these. One write names the partition, which does fire it.
    d.ctl
        .batch_execute(
            "update public.par_all set w = 11 where id = 1; \
             insert into public.par_all values (3, 30, 300); \
             delete from public.par_all where id = 2; \
             update public.par set x = 999 where id = 3",
        )
        .await
        .expect("write the to-side");
    capture_pass(&mut d).await;
    d.settle().await;

    d.ctl
        .batch_execute("alter table public.par_all detach partition public.par")
        .await
        .expect("detach the to-side");
    capture_pass(&mut d).await;
    d.settle().await;

    define_the_reader_and_compare(&mut d, true).await;
}

/// The same through inheritance: an `UPDATE` of the inheritance parent that
/// reaches its child's rows stages them as the parent's own, and a row
/// written to the child alone is never captured.
async fn inheritance_scenario(dropped_reader: bool) {
    let mut d = start(dropped_reader).await;
    d.ctl
        .batch_execute(
            "create table public.par_kid (id int primary key, w numeric, x numeric); \
             alter table public.par_kid inherit public.par",
        )
        .await
        .expect("make a child of the to-side");
    capture_pass(&mut d).await;
    assert_eq!(statuses(&d).await, ["(public.sg,live)"]);

    d.ctl
        .batch_execute(
            "insert into public.par_kid values (3, 30, 300), (4, 40, 400); \
             update public.par set w = w + 1; \
             delete from public.par where id = 2",
        )
        .await
        .expect("write the tree");
    capture_pass(&mut d).await;
    d.settle().await;

    d.ctl
        .batch_execute("alter table public.par_kid no inherit public.par")
        .await
        .expect("leave the tree");
    capture_pass(&mut d).await;
    d.settle().await;

    // With no earlier reader the projection has only keys, and the tree's
    // changes happen to leave them right: the stale part is the data a
    // reader brings.
    define_the_reader_and_compare(&mut d, dropped_reader).await;
}

#[tokio::test]
async fn a_reader_defined_after_the_to_side_left_a_partition_reads_the_to_side() {
    partition_scenario(false).await;
}

#[tokio::test]
async fn a_reader_defined_after_a_dropped_reader_and_a_partition_reads_the_to_side() {
    partition_scenario(true).await;
}

#[tokio::test]
async fn a_reader_defined_after_the_to_side_left_an_inheritance_tree_reads_the_to_side() {
    inheritance_scenario(false).await;
}

#[tokio::test]
async fn a_reader_defined_after_a_dropped_reader_and_an_inheritance_tree_reads_the_to_side() {
    inheritance_scenario(true).await;
}
