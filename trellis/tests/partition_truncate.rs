//! ADR-0017 P5 (issue #1064): a leaf or middle `TRUNCATE` queues that
//! node's correction, and a root `TRUNCATE` clears the target once.
//!
//! Each test builds a partitioned `public.items` and an aggregate over it,
//! kept live by the hand-driven pipeline (`support/drain_driver.rs`),
//! truncates part of the tree, drains the ring (whose seal queues the
//! correction) and runs the correction engine. The oracle is a from-scratch
//! `GROUP BY` over the source. Nothing waits for convergence (#297): the
//! pass, the drain, the engine and the merger are called by hand.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::capture::partitions::{self, PendingCorrection};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::staging::partition_correction::{self, Options, Outcome};

const SOURCE: &str = "public.items";
const DEFINITION: &str =
    "TRANSFORM agg FROM public.items GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";

/// `items` range-partitioned on `g`: leaves `items_p0`..`items_p3` hold the
/// groups 0-3, 4-7, 8-11 and 12-15. 160 rows, ten per group, `v` the id.
const FLAT: &str = "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by range (g); \
     create table public.items_p0 partition of public.items for values from (0) to (4); \
     create table public.items_p1 partition of public.items for values from (4) to (8); \
     create table public.items_p2 partition of public.items for values from (8) to (12); \
     create table public.items_p3 partition of public.items for values from (12) to (16); \
     insert into public.items select i, i % 16, i from generate_series(1, 160) i;";

/// The same rows under a middle: `items_m` holds the groups 0-7 in two list
/// leaves, `items_p2` and `items_p3` the rest.
const NESTED: &str = "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by range (g); \
     create table public.items_m partition of public.items for values from (0) to (8) \
         partition by list (g); \
     create table public.items_m0 partition of public.items_m for values in (0, 1, 2, 3); \
     create table public.items_m1 partition of public.items_m for values in (4, 5, 6, 7); \
     create table public.items_p2 partition of public.items for values from (8) to (12); \
     create table public.items_p3 partition of public.items for values from (12) to (16); \
     insert into public.items select i, i % 16, i from generate_series(1, 160) i;";

struct Fx {
    d: Driver,
}

impl Fx {
    async fn start(ddl: &str) -> Fx {
        let d = Driver::start(
            ddl,
            &[
                ("id", ValueType::Numeric),
                ("g", ValueType::Numeric),
                ("v", ValueType::Numeric),
            ],
            &[DEFINITION],
            &[SOURCE],
        )
        .await;
        let mut fx = Fx { d };
        // The first sight of the source records its tree and queues nothing.
        fx.pass().await;
        assert!(fx.queue().await.is_empty());
        assert_eq!(fx.wrong_groups().await, Vec::<i32>::new());
        fx
    }

    /// The capture half of one staging-worker pass.
    async fn pass(&mut self) {
        let desired = trellis::defs::tables_to_capture(self.d.pool())
            .await
            .expect("tables to capture");
        let outcome = trellis::capture::reconcile::reconcile(
            &mut self.d.ctl,
            DEFAULT_SCHEMA,
            &desired,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("pass");
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    }

    async fn queue(&self) -> Vec<PendingCorrection> {
        partitions::pending_corrections(&self.d.ctl, SOURCE)
            .await
            .expect("the queue")
    }

    /// The queue as `(kind, node, cause)`.
    async fn queued(&self) -> Vec<(String, String, String)> {
        self.queue()
            .await
            .into_iter()
            .map(|c| (c.kind, c.node_name, c.cause))
            .collect()
    }

    /// The groups whose target row differs from a `GROUP BY` over the
    /// source.
    async fn wrong_groups(&self) -> Vec<i32> {
        self.d
            .ctl
            .query(
                "with want as (select g, sum(v) as total, count(*) as n \
                               from public.items group by g), \
                      have as (select g, total, n from public.agg) \
                 select coalesce(w.g, h.g::integer) \
                 from want w full join have h on h.g = w.g \
                 where w.total is distinct from h.total or w.n is distinct from h.n \
                 order by 1",
                &[],
            )
            .await
            .expect("oracle")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// The target's row count.
    async fn target_rows(&self) -> i64 {
        self.d
            .ctl
            .query_one("select count(*) from public.agg", &[])
            .await
            .expect("target rows")
            .get(0)
    }

    async fn run(&self, options: &Options) -> Outcome {
        partition_correction::run_pending(self.d.pool(), options)
            .await
            .expect("run the pending corrections")
    }

    async fn merge(&mut self) {
        let plan = self.d.build_plan("agg").await;
        self.d.merge_all(&plan).await;
        self.d.settle().await;
    }

    /// Runs the queued corrections to their end and merges their deltas.
    async fn correct(&mut self) -> Outcome {
        let outcome = self.run(&Options::default()).await;
        self.merge().await;
        outcome
    }

    async fn exec(&self, sql: &str) {
        self.d
            .ctl
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn truncated(node: &str) -> (String, String, String) {
    (
        "truncated".to_string(),
        node.to_string(),
        "truncated".to_string(),
    )
}

fn range(from: i32, to: i32) -> Vec<i32> {
    (from..to).collect()
}

#[tokio::test]
async fn a_leaf_truncate_corrects_only_the_leafs_groups() {
    let mut fx = Fx::start(FLAT).await;
    // Rows written into the leaf after the truncate, in its transaction,
    // count.
    fx.exec(
        "begin; truncate public.items_p1; \
         insert into public.items values (1000, 5, 7), (1001, 6, 9); commit",
    )
    .await;
    fx.d.settle().await;
    assert_eq!(fx.queued().await, [truncated("public.items_p1")]);
    // No floor and no barrier: every other leaf's groups are exactly right,
    // and only the truncated leaf's are still counted.
    assert_eq!(fx.wrong_groups().await, range(4, 8));
    assert_eq!(fx.target_rows().await, 16);

    let outcome = fx.correct().await;
    assert_eq!(outcome.completed, 1);
    assert!(fx.queue().await.is_empty());
    assert_eq!(fx.wrong_groups().await, Vec::<i32>::new());
    // The emptied groups are gone; the two written after the truncate stay.
    assert_eq!(fx.target_rows().await, 14);
}

#[tokio::test]
async fn a_middle_truncate_corrects_the_middle_through_its_own_text() {
    let mut fx = Fx::start(NESTED).await;
    fx.exec("truncate public.items_m").await;
    fx.d.settle().await;
    // One correction, the middle's: its leaves' triggers fired under it and
    // staged nothing.
    assert_eq!(fx.queued().await, [truncated("public.items_m")]);
    assert_eq!(fx.wrong_groups().await, range(0, 8));

    fx.correct().await;
    assert!(fx.queue().await.is_empty());
    assert_eq!(fx.wrong_groups().await, Vec::<i32>::new());
    assert_eq!(fx.target_rows().await, 8);
}

#[tokio::test]
async fn a_root_truncate_clears_the_target_once_and_queues_nothing() {
    let mut fx = Fx::start(NESTED).await;
    fx.exec("truncate public.items").await;
    fx.d.settle().await;
    assert!(fx.queue().await.is_empty());
    assert_eq!(fx.wrong_groups().await, Vec::<i32>::new());
    assert_eq!(fx.target_rows().await, 0);
}

#[tokio::test]
async fn a_truncate_naming_several_nodes_queues_one_correction_per_node() {
    let mut fx = Fx::start(NESTED).await;
    fx.exec("truncate public.items_m0, public.items_p3").await;
    // `CASCADE` empties a leaf that references the table named.
    fx.exec(
        "create table public.parent (id integer primary key); \
         insert into public.parent select generate_series(0, 15); \
         alter table public.items_p2 add foreign key (g) references public.parent (id); \
         truncate public.parent cascade",
    )
    .await;
    fx.d.settle().await;
    let mut queued = fx.queued().await;
    queued.sort();
    assert_eq!(
        queued,
        [
            truncated("public.items_m0"),
            truncated("public.items_p2"),
            truncated("public.items_p3"),
        ]
    );
    let mut wrong = range(0, 4);
    wrong.extend(8..16);
    assert_eq!(fx.wrong_groups().await, wrong);

    let outcome = fx.correct().await;
    assert_eq!(outcome.completed, 3);
    assert_eq!(fx.wrong_groups().await, Vec::<i32>::new());
    assert_eq!(fx.target_rows().await, 4);
}

#[tokio::test]
async fn a_truncate_after_the_nodes_correction_started_queues_another() {
    let mut fx = Fx::start(FLAT).await;
    fx.exec("truncate public.items_p1").await;
    fx.d.settle().await;
    // A second truncate before the correction starts adds nothing.
    fx.exec("insert into public.items values (1000, 5, 7); truncate public.items_p1")
        .await;
    fx.d.settle().await;
    assert_eq!(fx.queued().await, [truncated("public.items_p1")]);

    // Start it: one page, past the fence.
    let options = Options {
        page_keys: 1,
        max_pages: Some(1),
    };
    let id = fx.queue().await[0].id;
    for _ in 0..10 {
        fx.run(&options).await;
        let walked: bool =
            fx.d.ctl
                .query_one(
                    "select exists (select 1 from partition_correction_progress \
                                where correction_id = $1 and keys_done > 0)",
                    &[&id],
                )
                .await
                .expect("progress")
                .get(0);
        if walked {
            break;
        }
    }
    fx.exec("insert into public.items values (1001, 6, 9); truncate public.items_p1")
        .await;
    fx.d.settle().await;
    assert_eq!(
        fx.queued().await,
        [truncated("public.items_p1"), truncated("public.items_p1")]
    );

    let outcome = fx.correct().await;
    assert_eq!(outcome.completed, 2);
    assert_eq!(fx.wrong_groups().await, Vec::<i32>::new());
    assert_eq!(fx.target_rows().await, 12);
}
