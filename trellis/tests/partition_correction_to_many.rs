//! ADR-0017 P4b (issue #1063): the correction engine for a to-many
//! relationship whose to-side is a partitioned source.
//!
//! Each test builds `public.orders` (the parents) and a partitioned
//! `public.items` (the children, partitioned on `order_id`), a definition
//! counting each order's items through a to-many relationship kept live by the
//! hand-driven pipeline (`support/drain_driver.rs`), runs one
//! partition-maintenance operation, lets the reconcile pass queue its
//! correction, and runs the engine (`run_pending`, which schedules the
//! correction's rows and claims them as a drain worker would). The oracle is
//! a from-scratch join of the two tables: before the correction the parents
//! the operation touched are wrong, after it and the drain of the recomputes
//! it staged none is. The engine's run stages the recomputes and leaves the
//! correction queued and the definition `backfilling` until the ring has
//! drained them; a run after the drain takes it off the queue. Nothing waits
//! for convergence (#297): the pass, the engine and the drain are called by
//! hand in the order the test wants.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::capture::partitions::{self, PendingCorrection};
use trellis::capture::reconcile::PassOutcome;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::partition_correction::{self, Options, Outcome};

const SOURCE: &str = "public.items";
const PARENTS: &str = "public.orders";
const RELATIONSHIP: &str = "RELATIONSHIP lines FROM orders.id TO items.order_id";
const DEFINITION: &str = "TRANSFORM totals FROM public.orders \
     SELECT label AS label, COUNT(lines.id) AS n, SUM(lines.v) AS total";

/// Orders 0-19; `items` partitioned `by` on `order_id`, ten items for each
/// of the orders 0-15 (`v` is the id). Orders 16-19 never have an item.
/// Leaves hold the orders 0-3, 4-7, 8-11 and 12-15 (by remainder for `HASH`).
fn tree(kind: &str) -> String {
    let leaves = match kind {
        "hash" => (0..4)
            .map(|r| {
                format!(
                    "create table public.items_p{r} partition of public.items \
                     for values with (modulus 4, remainder {r});"
                )
            })
            .collect::<String>(),
        "range" => (0..4)
            .map(|i| {
                format!(
                    "create table public.items_p{i} partition of public.items \
                     for values from ({}) to ({});",
                    i * 4,
                    i * 4 + 4
                )
            })
            .collect::<String>(),
        _ => (0..4)
            .map(|i| {
                format!(
                    "create table public.items_p{i} partition of public.items \
                     for values in ({}, {}, {}, {});",
                    i * 4,
                    i * 4 + 1,
                    i * 4 + 2,
                    i * 4 + 3
                )
            })
            .collect::<String>(),
    };
    with_leaves(&format!("partition by {kind} (order_id)"), &leaves)
}

/// The orders and an `items` root `partitioning` with `leaves`, then the
/// items.
fn with_leaves(partitioning: &str, leaves: &str) -> String {
    format!(
        "create table public.orders (id integer primary key, label integer); \
         insert into public.orders select i, i * 10 from generate_series(0, 19) i; \
         create table public.items (id integer not null, order_id integer not null, \
             v integer not null, primary key (order_id, id)) {partitioning}; \
         {leaves} \
         insert into public.items select i, i % 16, i from generate_series(1, 160) i;"
    )
}

struct Fx {
    d: Driver,
}

impl Fx {
    async fn start(ddl: &str) -> Fx {
        Self::start_with(ddl, &[DEFINITION]).await
    }

    async fn start_with(ddl: &str, definitions: &[&str]) -> Fx {
        let d = Driver::start_with_relationships(
            ddl,
            &[
                ("id", ValueType::Numeric),
                ("label", ValueType::Numeric),
                ("order_id", ValueType::Numeric),
                ("v", ValueType::Numeric),
            ],
            &[RELATIONSHIP],
            definitions,
            &[PARENTS, SOURCE],
        )
        .await;
        let mut fx = Fx { d };
        // The first sight of the source records its tree and queues nothing.
        fx.pass().await;
        assert!(fx.queue().await.is_empty());
        fx
    }

    /// The capture half of one staging-worker pass.
    async fn pass(&mut self) -> PassOutcome {
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
        outcome
    }

    async fn queue(&self) -> Vec<PendingCorrection> {
        partitions::pending_corrections(&self.d.ctl, SOURCE)
            .await
            .expect("the queue")
    }

    /// The orders whose target row differs from a join of the two tables, as
    /// `order: have vs want`.
    async fn wrong_parents(&self) -> Vec<String> {
        self.d
            .ctl
            .query(
                "with want as (select o.id, o.label, count(i.id) as n, sum(i.v) as total \
                               from public.orders o \
                               left join public.items i on i.order_id = o.id \
                               group by o.id, o.label), \
                      have as (select id, label, n, total from public.totals) \
                 select coalesce(w.id, h.id)::text || ': ' || coalesce(h.n::text, '-') \
                        || '/' || coalesce(h.total::text, '-') || ' vs ' \
                        || coalesce(w.n::text, '-') || '/' || coalesce(w.total::text, '-') \
                 from want w full join have h on h.id = w.id \
                 where w.id is null or h.id is null or w.label is distinct from h.label \
                    or w.n is distinct from h.n or w.total is distinct from h.total \
                 order by coalesce(w.id, h.id)",
                &[],
            )
            .await
            .expect("oracle")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// The orders whose count reads 0.
    async fn zero_parents(&self) -> Vec<i32> {
        self.d
            .ctl
            .query(
                "select id::integer from public.totals where n = 0 order by id",
                &[],
            )
            .await
            .expect("zero counts")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// Runs the pass, drains the ring and runs the engine through
    /// ([`Fx::run_through`]).
    async fn correct(&mut self) -> Outcome {
        self.pass().await;
        self.d.settle().await;
        self.run_through(&Options::default()).await
    }

    async fn run(&self, options: &Options) -> Outcome {
        partition_correction::run_pending(self.d.pool(), options)
            .await
            .expect("run the pending corrections")
    }

    /// Runs the engine with `options`, drains the recomputes it staged, and
    /// runs it again, which sees the drain and takes the corrections off the
    /// queue. The pages and keys are both runs', `completed` both runs' and
    /// `remaining` the second's.
    async fn run_through(&mut self, options: &Options) -> Outcome {
        let first = self.run(options).await;
        self.d.settle().await;
        let second = self.run(options).await;
        Outcome {
            pages: first.pages + second.pages,
            keys: first.keys + second.keys,
            completed: first.completed + second.completed,
            remaining: second.remaining,
        }
    }

    /// The catch-up a capture install on a new node parks for the definition
    /// (its to-side widened), run to `live`: the discharge stages its
    /// enumeration and the drain applies it. A `catching_up` definition gets
    /// its correction row only once it is `live`.
    async fn catch_up(&mut self) {
        for _ in 0..10 {
            if self.status("public.totals").await != "catching_up" {
                return;
            }
            trellis::intake::markers::run_pending_backfills(
                &mut self.d.ctl,
                drain_driver::WAKE,
                &trellis::staging::StagedWatermark::saturated(),
                Duration::ZERO,
            )
            .await
            .expect("discharge the catch-up");
            self.d.settle().await;
        }
        panic!("the definition is still catching_up");
    }

    /// Runs the engine while the definition catches up after a capture
    /// install (it waits for it), then catches it up and runs the engine
    /// through.
    async fn run_after_catch_up(&mut self) -> Outcome {
        if self.status("public.totals").await == "catching_up" {
            let waiting = self.run(&Options::default()).await;
            assert_eq!(waiting.completed, 0, "waits for the catch-up");
            assert_eq!(waiting.pages, 0, "waits for the catch-up");
            self.catch_up().await;
        }
        self.run_through(&Options::default()).await
    }

    async fn status(&self, target: &str) -> String {
        self.d
            .ctl
            .query_one(
                "select status from transform_definitions where target_table = $1",
                &[&target],
            )
            .await
            .expect("status")
            .get(0)
    }

    async fn exec(&self, sql: &str) {
        self.d
            .ctl
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    async fn definition_id(&self, target: &str) -> i64 {
        self.d
            .ctl
            .query_one(
                "select id from transform_definitions where target_table = $1",
                &[&target],
            )
            .await
            .expect("definition")
            .get(0)
    }

    /// The orders whose items a leaf holds.
    async fn orders_in(&self, leaf: &str) -> Vec<i32> {
        self.d
            .ctl
            .query(
                &format!("select distinct order_id from {leaf} order by order_id"),
                &[],
            )
            .await
            .expect("the leaf's orders")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }
}

fn queued(corrections: &[PendingCorrection]) -> Vec<(String, String)> {
    corrections
        .iter()
        .map(|c| (c.kind.clone(), c.node_name.clone()))
        .collect()
}

fn entry(kind: &str, node: &str) -> (String, String) {
    (kind.to_string(), node.to_string())
}

/// Seeded: the target starts equal to the join, the orders with no item at 0.
#[tokio::test]
async fn the_counts_start_right_and_childless_orders_read_zero() {
    let fx = Fx::start(&tree("hash")).await;
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
    assert_eq!(fx.zero_parents().await, [16, 17, 18, 19]);
}

/// A detach or a drop takes a leaf's items: their orders are overcounted
/// until the correction's recomputes drain, and then read 0, still present.
#[tokio::test]
async fn a_detached_or_dropped_leaf_leaves_its_orders_at_zero() {
    for kind in ["hash", "range", "list"] {
        for op in ["detach", "drop"] {
            let mut fx = Fx::start(&tree(kind)).await;
            let orders = fx.orders_in("public.items_p1").await;
            assert!(!orders.is_empty(), "{kind}: the leaf has orders");
            match op {
                "detach" => {
                    fx.exec("alter table public.items detach partition public.items_p1")
                        .await;
                }
                _ => fx.exec("drop table public.items_p1").await,
            }
            fx.pass().await;
            fx.d.settle().await;
            assert_eq!(
                queued(&fx.queue().await),
                [entry("gone_node", "public.items_p1")],
                "{kind} {op}"
            );
            assert!(
                !fx.wrong_parents().await.is_empty(),
                "{kind} {op}: the removed items are still counted"
            );
            let outcome = fx.run_through(&Options::default()).await;
            assert_eq!(outcome.completed, 1, "{kind} {op}");
            // The orders the leaf's bound routes: the leaf's own, with none
            // of the childless orders (16-19) in any leaf's bound but HASH's.
            let routed = i64::try_from(orders.len()).unwrap();
            if kind == "hash" {
                assert!(outcome.keys >= routed, "{kind} {op}: {outcome:?}");
            } else {
                assert_eq!(outcome.keys, routed, "{kind} {op}");
            }
            assert!(fx.queue().await.is_empty());
            assert_eq!(
                fx.wrong_parents().await,
                Vec::<String>::new(),
                "{kind} {op}"
            );
            let mut zero = orders.clone();
            zero.extend([16, 17, 18, 19]);
            zero.sort();
            assert_eq!(fx.zero_parents().await, zero, "{kind} {op}");
            let rows: i64 =
                fx.d.ctl
                    .query_one("select count(*) from public.totals", &[])
                    .await
                    .expect("count")
                    .get(0);
            assert_eq!(rows, 20, "{kind} {op}: no order's row is gone");
        }
    }
}

/// A leaf detached, corrected, changed and attached again: its orders are
/// undercounted until its correction drains.
#[tokio::test]
async fn an_attached_leaf_with_rows_is_counted_by_its_correction() {
    for kind in ["hash", "range", "list"] {
        let mut fx = Fx::start(&tree(kind)).await;
        fx.exec("alter table public.items detach partition public.items_p2")
            .await;
        fx.correct().await;
        assert_eq!(fx.wrong_parents().await, Vec::<String>::new(), "{kind}");
        fx.exec(
            "update public.items_p2 set v = v + 1000; delete from public.items_p2 where id % 3 = 0",
        )
        .await;
        let bound = match kind {
            "hash" => "for values with (modulus 4, remainder 2)",
            "range" => "for values from (8) to (12)",
            _ => "for values in (8, 9, 10, 11)",
        };
        fx.exec(&format!(
            "alter table public.items attach partition public.items_p2 {bound}"
        ))
        .await;
        fx.pass().await;
        fx.d.settle().await;
        assert_eq!(
            queued(&fx.queue().await),
            [entry("new_leaf", "public.items_p2")],
            "{kind}"
        );
        assert!(
            !fx.wrong_parents().await.is_empty(),
            "{kind}: the attached items are not counted yet"
        );
        let outcome = fx.run_after_catch_up().await;
        assert_eq!(outcome.completed, 1, "{kind}");
        assert_eq!(fx.wrong_parents().await, Vec::<String>::new(), "{kind}");
    }
}

/// A leaf created and written to directly, before any capture of it existed,
/// which no ring row records.
#[tokio::test]
async fn a_new_leaf_with_direct_writes_before_its_capture_is_counted_by_its_correction() {
    for kind in ["hash", "range", "list"] {
        let mut fx = Fx::start(&tree(kind)).await;
        let (clause, order) = match kind {
            "hash" => {
                let order: i32 =
                    fx.d.ctl
                        .query_one("select order_id from public.items_p3 limit 1", &[])
                        .await
                        .expect("an order of the leaf")
                        .get(0);
                ("for values with (modulus 4, remainder 3)", order)
            }
            "range" => ("for values from (12) to (16)", 13),
            _ => ("for values in (12, 13, 14, 15)", 13),
        };
        fx.exec("drop table public.items_p3").await;
        fx.exec(&format!(
            "create table public.items_new partition of public.items {clause}"
        ))
        .await;
        // Written into the leaf itself: its triggers do not exist yet.
        fx.exec(&format!(
            "insert into public.items_new values (1001, {order}, 7), (1002, {order}, 9)"
        ))
        .await;
        fx.pass().await;
        fx.d.settle().await;
        let queue = queued(&fx.queue().await);
        assert!(
            queue.contains(&entry("new_leaf", "public.items_new"))
                && queue.contains(&entry("gone_node", "public.items_p3")),
            "{kind}: {queue:?}"
        );
        let outcome = fx.run_after_catch_up().await;
        assert_eq!(outcome.completed, queue.len(), "{kind}");
        assert_eq!(fx.wrong_parents().await, Vec::<String>::new(), "{kind}");
        // Writes after the capture was installed count at once.
        fx.exec(&format!(
            "insert into public.items values (1003, {order}, 5)"
        ))
        .await;
        fx.d.settle().await;
        assert_eq!(fx.wrong_parents().await, Vec::<String>::new(), "{kind}");
    }
}

/// #1147: a new leaf under a to-many reader leaves it `live` through the pass
/// and parks no marker, whose discharge would stage a recompute for every
/// order of the source; the leaf's correction alone counts its items.
#[tokio::test]
async fn a_new_leaf_parks_no_marker_and_leaves_the_reader_live() {
    let mut fx = Fx::start(&tree("list")).await;
    fx.exec("drop table public.items_p3").await;
    fx.pass().await;
    fx.correct().await;
    let parked = "select generation::text from pending_backfill order by table_name";
    let before = fx.d.rows(parked).await;
    fx.exec(
        "create table public.items_new partition of public.items for values in (12, 13, 14, 15); \
         insert into public.items_new values (1001, 13, 7), (1002, 13, 9)",
    )
    .await;
    fx.pass().await;
    assert_eq!(fx.d.rows(parked).await, before, "no marker parked");
    assert_eq!(fx.status("public.totals").await, "live");
    fx.run_through(&Options::default()).await;
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
}

/// #1147: an install that adds a new leaf and also widens the capture, for a
/// reader registered in the same pass that reads a column the old functions
/// don't image, still parks the root's marker with its capture gate. The
/// gate holds the new reader until the rows the old functions staged, which
/// lack the column, have drained (`capture::reconcile`'s rule 3).
#[tokio::test]
async fn a_new_leaf_installed_with_a_widen_keeps_the_capture_gate() {
    let leaves = (0..4)
        .map(|i| {
            format!(
                "create table public.items_p{i} partition of public.items \
                 for values in ({}, {}, {}, {});",
                i * 4,
                i * 4 + 1,
                i * 4 + 2,
                i * 4 + 3
            )
        })
        .collect::<String>();
    let mut fx = Fx::start(&format!(
        "create table public.orders (id integer primary key, label integer); \
         insert into public.orders select i, i * 10 from generate_series(0, 19) i; \
         create table public.items (id integer not null, order_id integer not null, \
             v integer not null, w integer not null, primary key (order_id, id)) \
             partition by list (order_id); \
         {leaves} \
         insert into public.items select i, i % 16, i, 2 from generate_series(1, 160) i;"
    ))
    .await;
    // Staged by the old functions, which image no `w`, and left undrained.
    fx.exec("insert into public.items select 1000 + i, i % 16, i, 5 from generate_series(1, 32) i")
        .await;
    let columns: std::collections::HashMap<String, ValueType> = [
        ("id", ValueType::Numeric),
        ("label", ValueType::Numeric),
        ("order_id", ValueType::Numeric),
        ("v", ValueType::Numeric),
        ("w", ValueType::Numeric),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect();
    let widened = trellis::defs::install_definition(
        fx.d.pool(),
        "TRANSFORM widened FROM public.orders SELECT label AS label, SUM(lines.w) AS tw",
        &columns,
        "public",
    )
    .await
    .expect("define a reader of a new column")
    .id;
    fx.exec("create table public.items_p4 partition of public.items for values in (16, 17)")
        .await;
    let outcome = fx.pass().await;
    assert!(
        !outcome.ready.contains(&widened),
        "held by the gate: {:?}",
        outcome.ready
    );
    assert_eq!(
        fx.d.rows(
            "select table_name from pending_backfill where capture_gate_lsn is not null \
             order by 1"
        )
        .await,
        ["(public.items)"]
    );
}

/// A new sibling changes a `DEFAULT`'s text and takes no rows from it; a
/// dropped sibling's items go, and the `DEFAULT`'s text grows.
#[tokio::test]
async fn a_default_whose_siblings_changed_leaves_the_counts_right() {
    let mut fx = Fx::start(&with_leaves(
        "partition by list (order_id)",
        "create table public.items_a partition of public.items for values in (0, 1, 2, 3); \
         create table public.items_b partition of public.items for values in (4, 5, 6, 7); \
         create table public.items_d partition of public.items default;",
    ))
    .await;
    fx.exec("create table public.items_c partition of public.items for values in (99)")
        .await;
    fx.pass().await;
    let queue = queued(&fx.queue().await);
    assert!(
        queue.contains(&entry("new_leaf", "public.items_d")),
        "the default is treated as new: {queue:?}"
    );
    fx.d.settle().await;
    let outcome = fx.run_after_catch_up().await;
    assert_eq!(outcome.completed, queue.len());
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());

    fx.exec("drop table public.items_b").await;
    fx.correct().await;
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
    assert_eq!(fx.zero_parents().await, [4, 5, 6, 7, 16, 17, 18, 19]);
    assert!(fx.queue().await.is_empty());

    // The default itself goes: every order it held reads 0.
    fx.exec("drop table public.items_d").await;
    fx.correct().await;
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
    assert_eq!(
        fx.zero_parents().await,
        (4..20).collect::<Vec<i32>>(),
        "only the orders of items_a keep their items"
    );
}

/// A `DEFAULT` that is the only partition has no text: every order routes to
/// it, and dropping it leaves every order at 0.
#[tokio::test]
async fn a_lone_default_routes_every_order() {
    let mut fx = Fx::start(&with_leaves(
        "partition by list (order_id)",
        "create table public.items_d partition of public.items default;",
    ))
    .await;
    fx.exec("drop table public.items_d").await;
    fx.pass().await;
    fx.d.settle().await;
    let queue = fx.queue().await;
    assert_eq!(queue.len(), 1);
    assert!(queue[0].node_default && queue[0].constraint.is_none());
    let outcome = fx.run_through(&Options::default()).await;
    assert_eq!(outcome.keys, 20, "every order");
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
    assert_eq!(fx.zero_parents().await, (0..20).collect::<Vec<i32>>());
}

#[tokio::test]
async fn a_dropped_middle_is_corrected_through_its_own_text() {
    let mut fx = Fx::start(&with_leaves(
        "partition by range (order_id)",
        "create table public.items_m0 partition of public.items for values from (0) to (8) \
             partition by hash (order_id); \
         create table public.items_m0a partition of public.items_m0 \
             for values with (modulus 2, remainder 0); \
         create table public.items_m0b partition of public.items_m0 \
             for values with (modulus 2, remainder 1); \
         create table public.items_m1 partition of public.items for values from (8) to (16);",
    ))
    .await;
    fx.exec("drop table public.items_m0").await;
    fx.pass().await;
    fx.d.settle().await;
    assert_eq!(
        queued(&fx.queue().await),
        [entry("gone_node", "public.items_m0")],
        "the topmost gone node only"
    );
    assert!(!fx.wrong_parents().await.is_empty());
    let outcome = fx.run_through(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    assert_eq!(outcome.keys, 8, "the orders 0-7");
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
}

/// A new `HASH` leaf's correction routes by the leaf's text, which embeds
/// its middle's oid. A middle dropped, and the leaf with it, before the
/// correction has run makes that text raise `XX000`; the correction is
/// complete without work instead (#1148), and the middle's own `gone_node`,
/// queued by the next pass, recomputes its orders.
#[tokio::test]
async fn a_new_hash_leaf_whose_middle_was_dropped_completes_without_error() {
    let mut fx = Fx::start(&with_leaves(
        "partition by range (order_id)",
        "create table public.items_m0 partition of public.items for values from (0) to (8) \
             partition by hash (order_id); \
         create table public.items_m0a partition of public.items_m0 \
             for values with (modulus 2, remainder 0); \
         create table public.items_m0b partition of public.items_m0 \
             for values with (modulus 2, remainder 1); \
         create table public.items_m1 partition of public.items for values from (8) to (16);",
    ))
    .await;
    fx.exec("drop table public.items_m0b").await;
    fx.correct().await;
    fx.exec(
        "create table public.items_m0b partition of public.items_m0 \
         for values with (modulus 2, remainder 1)",
    )
    .await;
    fx.pass().await;
    fx.d.settle().await;
    assert_eq!(
        queued(&fx.queue().await),
        [entry("new_leaf", "public.items_m0b")]
    );
    // The capture install on the new leaf parks a catch-up; the definition
    // gets its correction row once it is `live`.
    fx.catch_up().await;
    // Before the leaf's correction runs, its middle goes too.
    fx.exec("drop table public.items_m0").await;
    let outcome = fx.run_through(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    assert!(fx.queue().await.is_empty());
    fx.correct().await;
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
}

/// Item changes staged before the drop and drained after the correction
/// recompute their orders from the live rows: they bring no item back.
#[tokio::test]
async fn a_change_staged_before_the_ddl_does_not_resurrect_the_dropped_items() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec(
        "update public.items set v = v + 1 where order_id in (4, 5); \
         insert into public.items values (3001, 6, 11)",
    )
    .await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    // The engine runs with the ring undrained.
    let outcome = fx.run_through(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
    assert_eq!(fx.zero_parents().await, [4, 5, 6, 7, 16, 17, 18, 19]);
}

/// The code never deletes: a trigger on the target raises on any `DELETE`
/// while the engine runs and its recomputes drain.
#[tokio::test]
async fn the_correction_issues_no_delete_against_the_target() {
    let mut fx = Fx::start(&tree("hash")).await;
    fx.exec("drop table public.items_p0").await;
    fx.pass().await;
    fx.d.settle().await;
    fx.exec(
        "create function public.no_delete() returns trigger language plpgsql as \
         $$ begin raise exception 'a correction deleted from %', tg_table_name; end $$; \
         create trigger no_delete before delete on public.totals \
             for each statement execute function public.no_delete()",
    )
    .await;
    let outcome = fx.run_through(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());

    let source = include_str!("../src/staging/partition_correction/to_many.rs");
    for forbidden in ["delete from", "truncate "] {
        assert!(
            source
                .lines()
                .filter(|l| !l.trim_start().starts_with("//"))
                .all(|l| !l.to_lowercase().contains(forbidden)),
            "the engine's code mentions {forbidden:?}"
        );
    }
}

/// Stopped after two pages and run again: the second run starts after the
/// first run's cursor, the progress counts the keys remaining, and the
/// orders staged across both are the routed set.
#[tokio::test]
async fn a_correction_resumes_after_its_last_committed_page() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    fx.d.settle().await;
    let correction = fx.queue().await.remove(0);
    let definition = fx.definition_id("public.totals").await;

    let first = fx
        .run(&Options {
            page_keys: 1,
            max_pages: Some(2),
        })
        .await;
    assert_eq!(first.pages, 2);
    assert_eq!(first.keys, 2);
    assert_eq!(first.completed, 0);
    assert_eq!(first.remaining, 1);
    assert_eq!(
        fx.queue().await.len(),
        1,
        "still queued: a restart finds it"
    );
    let saved = partition_correction::progress(&fx.d.ctl, correction.id, definition)
        .await
        .expect("progress")
        .expect("the progress is committed");
    assert_eq!(saved.phase(), "parents");
    assert_eq!(saved.keys_done, 2);
    assert_eq!(
        saved.cursor.as_deref(),
        Some("5"),
        "orders 4 and 5 are staged"
    );
    assert_eq!(saved.keys_total, Some(4), "the orders 4-7");
    assert_eq!(saved.keys_remaining(), Some(2));

    // The staged recomputes drain on their own; the rest wait.
    fx.d.settle().await;
    let wrong = fx.wrong_parents().await;
    assert_eq!(wrong.len(), 2, "orders 6 and 7: {wrong:?}");

    // The restart: a new call, nothing but the committed progress.
    let rest = fx
        .run_through(&Options {
            page_keys: 1,
            max_pages: None,
        })
        .await;
    assert_eq!(rest.completed, 1);
    assert_eq!(rest.keys, 2, "it starts after the last committed page");
    assert!(fx.queue().await.is_empty());
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
}

/// An aggregate over the items and the to-many counts read the same source:
/// the correction leaves the queue only once both are corrected. The
/// aggregate is on the group basis, whose build over a partitioned source with
/// rows needs the tree recorded, so it is defined after the first pass.
#[tokio::test]
async fn an_aggregate_and_a_to_many_reader_are_both_corrected_before_the_queue_clears() {
    const AGGREGATE: &str = "TRANSFORM agg FROM public.items GROUP BY order_id WITHOUT SOURCE INDEX SELECT COUNT(*) AS n";
    let mut fx = Fx::start(&tree("list")).await;
    trellis::defs::install_definition(
        fx.d.pool(),
        AGGREGATE,
        &[
            ("id", ValueType::Integer(IntWidth::Int4)),
            ("order_id", ValueType::Integer(IntWidth::Int4)),
            ("v", ValueType::Integer(IntWidth::Int4)),
        ]
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect(),
        "public",
    )
    .await
    .expect("define the aggregate");
    trellis::intake::markers::settle_registrations(fx.d.pool()).await;
    fx.d.settle().await;
    assert_eq!(
        fx.d.rows("select status from transform_definitions where status <> 'live'")
            .await,
        Vec::<String>::new(),
        "both definitions are live before the scenario"
    );
    fx.exec("drop table public.items_p2").await;
    fx.pass().await;
    fx.d.settle().await;
    // One page: the aggregate's, so the to-many reader has not run and the
    // correction stays.
    let first = fx
        .run(&Options {
            page_keys: 10_000,
            max_pages: Some(1),
        })
        .await;
    assert_eq!(first.completed, 0);
    assert_eq!(fx.queue().await.len(), 1);
    let outcome = fx.run_through(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    fx.d.settle().await;
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
    let wrong: i64 =
        fx.d.ctl
            .query_one(
                "select count(*) from (select order_id, count(*) as n from public.items \
                                   group by order_id) w \
             full join public.agg a on a.order_id = w.order_id \
             where w.n is distinct from a.n",
                &[],
            )
            .await
            .expect("oracle")
            .get(0);
    assert_eq!(wrong, 0);
}

/// A gone node with no usable text routes no order: its detached table's
/// rows are read instead, a few at a time, and their orders recomputed.
#[tokio::test]
async fn a_gone_node_without_a_text_has_its_detached_rows_read() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("alter table public.items detach partition public.items_p1")
        .await;
    fx.pass().await;
    fx.d.settle().await;
    // The text the pass could not record (the node left the tree between
    // its tree read and its text fetch).
    fx.exec("update partition_corrections set constraint_text = null")
        .await;
    let correction = fx.queue().await.remove(0);
    let definition = fx.definition_id("public.totals").await;
    let mut seen_node_cursor = false;
    let mut staged = 0;
    for _ in 0..100 {
        let outcome = fx
            .run(&Options {
                page_keys: 3,
                max_pages: Some(1),
            })
            .await;
        staged += outcome.keys;
        if outcome.completed == 1 {
            break;
        }
        // A page's recomputes drain, which the last page's `drain` phase
        // waits for.
        fx.d.settle().await;
        if let Some(progress) = partition_correction::progress(&fx.d.ctl, correction.id, definition)
            .await
            .expect("progress")
            && progress.phase() == "node"
            && progress.cursor.is_some()
        {
            seen_node_cursor = true;
        }
    }
    assert!(seen_node_cursor, "a page ended inside the node phase");
    assert!(fx.queue().await.is_empty());
    assert!(staged >= 4, "the orders 4-7, once per page that names them");
    fx.d.settle().await;
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
    assert_eq!(fx.zero_parents().await, [4, 5, 6, 7, 16, 17, 18, 19]);
}

/// A detach that is still pending: the correction starts only once the
/// `pg_inherits` row is gone.
#[tokio::test]
async fn a_detach_pending_leaf_waits_for_the_detach_to_finalize() {
    let mut fx = Fx::start(&tree("range")).await;
    let holder = drain_driver::connect(fx.d.db.dsn()).await;
    holder
        .batch_execute("begin; lock table public.items_p2 in access share mode")
        .await
        .expect("hold the leaf");
    let detacher = drain_driver::connect(fx.d.db.dsn()).await;
    detacher
        .batch_execute("set statement_timeout = '1500ms'")
        .await
        .expect("timeout");
    let interrupted = detacher
        .batch_execute("alter table public.items detach partition public.items_p2 concurrently")
        .await;
    assert!(interrupted.is_err(), "the second phase waits on the holder");

    fx.pass().await;
    fx.d.settle().await;
    assert_eq!(
        queued(&fx.queue().await),
        [entry("gone_node", "public.items_p2")]
    );
    let waiting = fx.run(&Options::default()).await;
    assert_eq!(waiting.completed, 0);
    assert_eq!(waiting.remaining, 1);
    assert_eq!(
        waiting.pages, 0,
        "nothing started while the detach is pending"
    );

    holder.batch_execute("rollback").await.expect("release");
    fx.exec("alter table public.items detach partition public.items_p2 finalize")
        .await;
    fx.pass().await;
    fx.d.settle().await;
    let outcome = fx.run_through(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
}

/// The orders' join column re-typed after the pass queued the correction,
/// with an order the items' key type can't hold: the reader waits, rather
/// than failing the run and every other correction of the source with it.
/// (The next pass pauses the definition, which takes it off the readers.)
#[tokio::test]
async fn a_join_column_retyped_since_the_pass_waits_instead_of_failing_the_run() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    fx.d.settle().await;
    fx.exec(
        "alter table public.orders alter column id type bigint; \
         insert into public.orders values (3000000000, 1)",
    )
    .await;
    let outcome = fx.run(&Options::default()).await;
    assert_eq!(outcome.completed, 0);
    assert_eq!(outcome.remaining, 1);
    assert_eq!(outcome.pages, 0, "no page ran");
    assert_eq!(fx.queue().await.len(), 1, "the correction stays queued");
}

/// The engine's run stages the routed orders' recomputes and counts them
/// done, but the target is not right until the ring applies them: the
/// definition reads `backfilling` and the correction stays queued (the
/// progress in its `drain` phase) until the ring has drained through the
/// segment its last page wrote to. Only then does a run move it `live`.
#[tokio::test]
async fn a_to_many_reader_reads_backfilling_until_its_recomputes_drain() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    fx.d.settle().await;
    let correction = fx.queue().await.remove(0);
    let definition = fx.definition_id("public.totals").await;
    assert_eq!(fx.status("public.totals").await, "live");

    let staged = fx.run(&Options::default()).await;
    assert_eq!(staged.keys, 4, "the orders 4-7");
    assert_eq!(staged.completed, 0);
    assert_eq!(staged.remaining, 1);
    let progress = partition_correction::progress(&fx.d.ctl, correction.id, definition)
        .await
        .expect("progress")
        .expect("started");
    assert_eq!(progress.phase(), "drain");
    assert_eq!(progress.keys_remaining(), Some(0), "every recompute staged");
    assert!(
        !fx.wrong_parents().await.is_empty(),
        "the recomputes are not applied yet"
    );
    assert_eq!(
        fx.status("public.totals").await,
        "backfilling",
        "not live while its target is wrong"
    );
    assert!(
        !trellis::staging::build::try_complete(fx.d.pool(), definition)
            .await
            .expect("try_complete"),
        "the build is not done while the correction's row waits"
    );
    // Run again before the drain: it still waits.
    let waiting = fx.run(&Options::default()).await;
    assert_eq!((waiting.completed, waiting.remaining), (0, 1));
    assert_eq!(fx.status("public.totals").await, "backfilling");

    fx.d.settle().await;
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
    let done = fx.run(&Options::default()).await;
    assert_eq!((done.completed, done.remaining), (1, 0));
    assert!(fx.queue().await.is_empty());
    assert_eq!(fx.status("public.totals").await, "live");
}

/// A to-many reader under an old-path build (`backfilling`, no Re-derive
/// build recorded: `build` NULL) holds the queued correction instead of
/// panicking on the NULL its applying predicate would otherwise select.
#[tokio::test]
async fn a_to_many_reader_under_an_old_path_build_holds_the_correction() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    fx.d.settle().await;
    fx.exec(
        "update transform_definitions set status = 'backfilling', build = null \
         where target_table = 'public.totals'",
    )
    .await;
    let held = fx.run(&Options::default()).await;
    assert_eq!((held.completed, held.remaining), (0, 1));
}

/// The staging worker's whole pass schedules the to-many reader's row and
/// moves it to `backfilling` in the pass that detects the drop, as it does
/// an aggregate's.
#[tokio::test]
async fn the_pass_that_detects_the_drop_schedules_the_to_many_reader() {
    let mut fx = Fx::start(&tree("list")).await;
    fx.exec("drop table public.items_p3").await;
    let pool = fx.d.pool().clone();
    trellis::client::reconcile_pass(
        &mut fx.d.ctl,
        &pool,
        DEFAULT_SCHEMA,
        drain_driver::WAKE,
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
    let definition = fx.definition_id("public.totals").await;
    let rows: i64 =
        fx.d.ctl
            .query_one(
                "select count(*) from backfill_chunks \
             where definition_id = $1 and kind = 'correction' and not done",
                &[&definition],
            )
            .await
            .expect("rows")
            .get(0);
    assert_eq!(rows, 1, "one correction row for the to-many reader");
    assert_eq!(fx.status("public.totals").await, "backfilling");
    fx.d.settle().await;
    let outcome = fx.run_through(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    assert_eq!(fx.status("public.totals").await, "live");
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
}

/// A leaf `TRUNCATE` queues the leaf's correction at the seal (P5): its
/// orders are recomputed through the leaf's text, and read 0.
#[tokio::test]
async fn a_truncated_leaf_leaves_its_orders_at_zero() {
    for kind in ["hash", "range", "list"] {
        let mut fx = Fx::start(&tree(kind)).await;
        let orders = fx.orders_in("public.items_p1").await;
        fx.exec("truncate public.items_p1").await;
        fx.d.settle().await;
        assert_eq!(
            queued(&fx.queue().await),
            [entry("truncated", "public.items_p1")],
            "{kind}"
        );
        let outcome = fx.run_through(&Options::default()).await;
        assert_eq!(outcome.completed, 1, "{kind}");
        assert_eq!(fx.wrong_parents().await, Vec::<String>::new(), "{kind}");
        let mut zero = orders.clone();
        zero.extend([16, 17, 18, 19]);
        zero.sort();
        assert_eq!(fx.zero_parents().await, zero, "{kind}");
    }
}

/// The last page of a walk stages nothing when the routed parents fill the
/// pages before it exactly, or when no parent routes to the node (a new,
/// empty partition). If the ring sealed and drained the earlier pages'
/// recomputes in between, the segment active at that page's commit is empty.
/// The staging worker seals an empty segment only while its predecessor's
/// slot still holds rows (`seal_if_active_nonempty`'s idle seal), so on an
/// idle ring nothing need ever seal it. The correction finishes without any
/// seal: nothing it staged can be in that empty segment.
#[tokio::test]
async fn a_correction_whose_last_page_stages_nothing_finishes_on_an_idle_ring() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    fx.d.settle().await;
    // The orders 4-7 fill two pages of two; a third finds none.
    let first = fx
        .run(&Options {
            page_keys: 2,
            max_pages: Some(2),
        })
        .await;
    assert_eq!((first.pages, first.keys), (2, 4));
    // Their recomputes seal and drain; the active segment is now empty.
    fx.d.settle().await;
    assert_eq!(fx.wrong_parents().await, Vec::<String>::new());
    let last = fx
        .run(&Options {
            page_keys: 2,
            max_pages: None,
        })
        .await;
    // No seal runs between the two: the correction must not need one.
    let again = fx.run(&Options::default()).await;
    assert_eq!(
        last.completed + again.completed,
        1,
        "the correction finishes without traffic to seal its empty segment"
    );
    assert!(fx.queue().await.is_empty());
    assert_eq!(fx.status("public.totals").await, "live");
}
