//! #958 (X12-7d): a renamed partition-key column under a ledger aggregate.
//!
//! The column check (#622) pauses the reader at the next pass: a rename
//! changes no lifecycle fingerprint, so nothing is queued, and the recorded
//! routing texts still name the old column. Restoring the column and
//! `RESUME` rebuild the aggregate from the table, which covers a leaf
//! dropped while the reader was paused, and the stale texts are re-recorded.
//! Nothing waits for convergence (#297).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::capture::partitions;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;

const SOURCE: &str = "public.items";
const DEFINITION: &str =
    "TRANSFORM agg FROM public.items GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";

const TREE: &str = "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by range (g); \
     create table public.items_p0 partition of public.items for values from (0) to (8); \
     create table public.items_p1 partition of public.items for values from (8) to (16); \
     insert into public.items select i, i % 16, i from generate_series(1, 160) i;";

async fn pass(d: &mut Driver) {
    let desired = trellis::defs::tables_to_capture(d.pool())
        .await
        .expect("tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        &mut d.ctl,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn status(d: &Driver) -> String {
    d.ctl
        .query_one(
            "select status from transform_definitions where target_table = 'public.agg'",
            &[],
        )
        .await
        .expect("status")
        .get(0)
}

async fn wrong_groups(d: &Driver) -> Vec<String> {
    d.rows(
        "with want as (select g, sum(v) as total, count(*) as n \
                       from public.items group by g), \
              have as (select g, total, n from public.agg) \
         select coalesce(w.g, h.g)::text from want w full join have h on h.g = w.g \
         where w.total is distinct from h.total or w.n is distinct from h.n \
         order by 1",
    )
    .await
}

#[tokio::test]
async fn a_renamed_partition_key_column_pauses_the_reader_and_resume_rebuilds() {
    trellis::defs::partition_tree::open_gate_for_tests();
    let mut d = Driver::start(
        TREE,
        &[
            ("id", ValueType::Numeric),
            ("g", ValueType::Numeric),
            ("v", ValueType::Numeric),
        ],
        &[DEFINITION],
        &[SOURCE],
    )
    .await;
    pass(&mut d).await;
    assert_eq!(wrong_groups(&d).await, Vec::<String>::new());

    d.ctl
        .batch_execute("alter table public.items rename column g to gg")
        .await
        .expect("rename the key column");
    pass(&mut d).await;
    assert_eq!(status(&d).await, "paused");
    assert!(
        partitions::pending_corrections(&d.ctl, SOURCE)
            .await
            .expect("queue")
            .is_empty(),
        "a renamed column is no tree change"
    );

    // Paused: a leaf dropped, and the column restored.
    d.ctl
        .batch_execute("drop table public.items_p1; alter table public.items rename column gg to g")
        .await
        .expect("drop and restore");
    pass(&mut d).await;
    assert_eq!(status(&d).await, "paused", "a pass leaves it paused");

    let trellis = trellis::Trellis::connect(
        trellis::Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        trellis::TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis.apply("RESUME TRANSFORM agg").await.expect("resume");
    pass(&mut d).await;
    trellis::intake::markers::settle_registrations(d.pool()).await;
    d.settle().await;
    assert_eq!(status(&d).await, "live");
    assert_eq!(wrong_groups(&d).await, Vec::<String>::new());
    // The tree is recorded again: a later leaf drop is still corrected.
    d.ctl
        .batch_execute("drop table public.items_p0")
        .await
        .expect("drop the remaining leaf");
    pass(&mut d).await;
    let queued = partitions::pending_corrections(&d.ctl, SOURCE)
        .await
        .expect("queue");
    assert_eq!(queued.len(), 1, "{queued:?}");
    trellis.shutdown().await.expect("shutdown");
}
