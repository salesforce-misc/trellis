//! ADR-0017 P4a (issue #1062): the correction engine for an aggregate over a
//! partitioned source.
//!
//! Each test builds a partitioned `public.items`, an aggregate over it kept
//! live by the hand-driven pipeline (`support/drain_driver.rs`), runs one
//! partition-maintenance operation, lets the reconcile pass queue its
//! correction, and runs the engine. The oracle is a from-scratch `GROUP BY`
//! over the source: before the correction the groups the operation touched
//! are wrong, after it and the merge of its deltas none is. Nothing waits for
//! convergence (#297): the pass, the engine and the merger are called by hand
//! in the order the test wants.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::capture::partitions::{self, PendingCorrection};
use trellis::capture::reconcile::PassOutcome;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::staging::partition_correction::{self, Options, Outcome};

const SOURCE: &str = "public.items";
const DEFINITION: &str =
    "TRANSFORM agg FROM public.items GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";

/// `items` partitioned `by` on `g`, with the primary key a partitioned table
/// needs. Leaves hold the groups 0-3, 4-7, 8-11 and 12-15.
fn tree(kind: &str) -> String {
    let head = format!(
        "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by {kind} (g);"
    );
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
    // 160 rows: ten per group of sixteen groups, `v` is the id.
    format!(
        "{head}{leaves} insert into public.items select i, i % 16, i from \
         generate_series(1, 160) i;"
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
        trellis::defs::partition_tree::open_gate_for_tests();
        let d = Driver::start(
            ddl,
            &[
                ("id", ValueType::Numeric),
                ("g", ValueType::Numeric),
                ("h", ValueType::Numeric),
                ("v", ValueType::Numeric),
            ],
            definitions,
            &[SOURCE],
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

    /// The groups whose target row differs from a `GROUP BY` over the source,
    /// as `group: have vs want`.
    async fn wrong_groups(&self) -> Vec<String> {
        self.d
            .ctl
            .query(
                "with want as (select g, sum(v) as total, count(*) as n \
                               from public.items group by g), \
                      have as (select g, total, n from public.agg) \
                 select coalesce(w.g, h.g)::text || ': ' || coalesce(h.total::text, '-') \
                        || '/' || coalesce(h.n::text, '-') || ' vs ' \
                        || coalesce(w.total::text, '-') || '/' || coalesce(w.n::text, '-') \
                 from want w full join have h on h.g = w.g \
                 where w.total is distinct from h.total or w.n is distinct from h.n \
                 order by coalesce(w.g, h.g)",
                &[],
            )
            .await
            .expect("oracle")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// Runs the pass, drains the ring, runs the engine to its end and merges
    /// the deltas it left.
    async fn correct(&mut self) -> Outcome {
        self.pass().await;
        self.d.settle().await;
        let outcome = self.run(&Options::default()).await;
        self.merge().await;
        outcome
    }

    async fn run(&self, options: &Options) -> Outcome {
        partition_correction::run_pending(self.d.pool(), SOURCE, options)
            .await
            .expect("run the pending corrections")
    }

    async fn merge(&mut self) {
        let plan = self.d.build_plan("agg").await;
        self.d.merge_all(&plan).await;
        self.d.settle().await;
    }

    async fn exec(&self, sql: &str) {
        self.d
            .ctl
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
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

/// Seeded: the aggregate starts equal to the source.
#[tokio::test]
async fn the_aggregate_starts_right_and_its_ledger_has_the_group_index() {
    let fx = Fx::start(&tree("hash")).await;
    assert!(fx.wrong_groups().await.is_empty());
    let indexed: i64 =
        fx.d.ctl
            .query_one(
                "select count(*) from pg_indexes where tablename = 'agg__ledger' \
               and indexdef like '%(g)%' and indexdef like '%__member%'",
                &[],
            )
            .await
            .expect("indexes")
            .get(0);
    assert_eq!(
        indexed, 1,
        "an invertible aggregate over a partitioned source keeps its GROUP BY index"
    );
}

#[tokio::test]
async fn a_detached_or_dropped_leaf_is_overcounted_until_its_correction_drains() {
    for kind in ["hash", "range", "list"] {
        for op in ["detach", "drop"] {
            let mut fx = Fx::start(&tree(kind)).await;
            let rows: i64 =
                fx.d.ctl
                    .query_one("select count(*) from public.items_p1", &[])
                    .await
                    .expect("the leaf's rows")
                    .get(0);
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
                !fx.wrong_groups().await.is_empty(),
                "{kind} {op}: the removed rows are still counted"
            );
            let outcome = fx.run(&Options::default()).await;
            assert_eq!(outcome.completed, 1, "{kind} {op}");
            // The leaf's live entries, once each: a detached table's rows are
            // not walked again (the text found their entries).
            assert_eq!(outcome.keys, rows, "{kind} {op}");
            assert!(fx.queue().await.is_empty());
            // The correction wrote deltas; until the merger folds them the
            // groups are as they were.
            assert!(!fx.wrong_groups().await.is_empty());
            fx.merge().await;
            assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{kind} {op}");
        }
    }
}

#[tokio::test]
async fn an_attached_leaf_with_rows_is_undercounted_until_its_correction_drains() {
    for kind in ["hash", "range", "list"] {
        let mut fx = Fx::start(&tree(kind)).await;
        // Take a leaf out, correct, and put a modified copy back.
        fx.exec("alter table public.items detach partition public.items_p2")
            .await;
        fx.correct().await;
        assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{kind}");
        fx.exec("update public.items_p2 set v = v + 1000").await;
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
            !fx.wrong_groups().await.is_empty(),
            "{kind}: the attached rows are not counted yet"
        );
        let outcome = fx.run(&Options::default()).await;
        assert_eq!(outcome.completed, 1, "{kind}");
        fx.merge().await;
        assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{kind}");
    }
}

/// A leaf created and written to directly, before any capture of it existed,
/// which no ring row records.
#[tokio::test]
async fn a_new_leaf_with_direct_writes_before_its_capture_is_counted_by_its_correction() {
    for kind in ["hash", "range", "list"] {
        let mut fx = Fx::start(&tree(kind)).await;
        // The replacement of a dropped leaf, so a hash tree has the remainder
        // free and a range or list tree the bounds.
        let (clause, group) = match kind {
            "hash" => {
                let group: i32 =
                    fx.d.ctl
                        .query_one("select g from public.items_p3 limit 1", &[])
                        .await
                        .expect("a group of the leaf")
                        .get(0);
                ("for values with (modulus 4, remainder 3)", group)
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
            "insert into public.items_new values (1001, {group}, 7), (1002, {group}, 9)"
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
        let outcome = fx.run(&Options::default()).await;
        assert_eq!(outcome.completed, queue.len(), "{kind}");
        fx.merge().await;
        assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{kind}");
        // Writes after the capture was installed count at once.
        fx.exec(&format!(
            "insert into public.items values (1003, {group}, 5)"
        ))
        .await;
        fx.d.settle().await;
        assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{kind}");
    }
}

#[tokio::test]
async fn a_dropped_middle_is_corrected_through_its_own_text() {
    let mut fx = Fx::start(
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_m0 partition of public.items for values from (0) to (8) \
             partition by hash (g); \
         create table public.items_m0a partition of public.items_m0 \
             for values with (modulus 2, remainder 0); \
         create table public.items_m0b partition of public.items_m0 \
             for values with (modulus 2, remainder 1); \
         create table public.items_m1 partition of public.items for values from (8) to (16); \
         insert into public.items select i, i % 16, i from generate_series(1, 160) i;",
    )
    .await;
    fx.exec("drop table public.items_m0").await;
    fx.pass().await;
    fx.d.settle().await;
    assert_eq!(
        queued(&fx.queue().await),
        [entry("gone_node", "public.items_m0")],
        "the topmost gone node only"
    );
    assert!(!fx.wrong_groups().await.is_empty());
    let outcome = fx.run(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    fx.merge().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// A new sibling changes a `DEFAULT`'s text; its groups are unchanged, and the
/// correction leaves them right.
#[tokio::test]
async fn a_default_whose_siblings_changed_and_a_dropped_sibling_leave_the_groups_right() {
    let mut fx = Fx::start(
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by list (g); \
         create table public.items_a partition of public.items for values in (0, 1, 2, 3); \
         create table public.items_b partition of public.items for values in (4, 5, 6, 7); \
         create table public.items_d partition of public.items default; \
         insert into public.items select i, i % 16, i from generate_series(1, 160) i;",
    )
    .await;
    // A sibling for groups the default holds no rows of.
    fx.exec("create table public.items_c partition of public.items for values in (99)")
        .await;
    fx.pass().await;
    let queue = queued(&fx.queue().await);
    assert!(
        queue.contains(&entry("new_leaf", "public.items_d")),
        "the default is treated as new: {queue:?}"
    );
    fx.d.settle().await;
    let outcome = fx.run(&Options::default()).await;
    assert_eq!(outcome.completed, queue.len());
    fx.merge().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());

    // A dropped sibling takes its rows; the default's text grows.
    fx.exec("drop table public.items_b").await;
    fx.correct().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
    assert!(fx.queue().await.is_empty());
}

/// A `DEFAULT` that is the only partition has no text: every group routes to
/// it, and dropping it retracts all of them.
#[tokio::test]
async fn a_lone_default_has_no_text_and_every_group_routes_to_it() {
    let mut fx = Fx::start(
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by list (g); \
         create table public.items_d partition of public.items default; \
         insert into public.items select i, i % 16, i from generate_series(1, 160) i;",
    )
    .await;
    fx.exec("drop table public.items_d").await;
    fx.pass().await;
    fx.d.settle().await;
    let queue = fx.queue().await;
    assert_eq!(queue.len(), 1);
    assert!(queue[0].node_default && queue[0].constraint.is_none());
    fx.run(&Options::default()).await;
    fx.merge().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
    let rows: i64 =
        fx.d.ctl
            .query_one("select count(*) from public.agg", &[])
            .await
            .expect("count")
            .get(0);
    assert_eq!(rows, 0, "every group is gone");
}

/// A write through the parent has no window: a group no operation touched is
/// right as soon as the ring drains, with the correction still queued.
#[tokio::test]
async fn writes_through_the_parent_have_no_window() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("alter table public.items detach partition public.items_p0")
        .await;
    fx.pass().await;
    fx.exec(
        "insert into public.items values (2001, 9, 100); delete from public.items where id = 10",
    )
    .await;
    fx.d.settle().await;
    let wrong = fx.wrong_groups().await;
    assert!(
        wrong.iter().all(|g| {
            let group: i32 = g.split(':').next().unwrap().parse().unwrap();
            group < 4
        }),
        "only the detached leaf's groups are wrong: {wrong:?}"
    );
    assert!(!fx.queue().await.is_empty());
}

/// A ring change staged before the DDL and drained after the correction must
/// not bring the removed rows back.
#[tokio::test]
async fn a_change_staged_before_the_ddl_does_not_resurrect_the_dropped_rows() {
    let mut fx = Fx::start(&tree("range")).await;
    // Staged, not drained: updates to the groups of the leaf about to go.
    fx.exec(
        "update public.items set v = v + 1 where g in (4, 5); \
             insert into public.items values (3001, 6, 11)",
    )
    .await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    // The engine runs with the ring undrained.
    let outcome = fx.run(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    fx.merge().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// The code never deletes: a trigger on the target, the ledger and the
/// deltas raises on any `DELETE`, and is in place while the engine runs.
#[tokio::test]
async fn the_correction_issues_no_delete_against_the_target_or_the_ledger() {
    let mut fx = Fx::start(&tree("hash")).await;
    fx.exec("drop table public.items_p0").await;
    fx.pass().await;
    fx.d.settle().await;
    let guard = "create function public.no_delete() returns trigger language plpgsql as \
                 $$ begin raise exception 'a correction deleted from %', tg_table_name; end $$; \
                 create trigger no_delete before delete on public.agg \
                     for each statement execute function public.no_delete(); \
                 create trigger no_delete before delete on public.agg__ledger \
                     for each statement execute function public.no_delete(); \
                 create trigger no_delete before delete on public.agg__deltas \
                     for each statement execute function public.no_delete()";
    fx.exec(guard).await;
    let outcome = fx.run(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    fx.exec(
        "drop trigger no_delete on public.agg; drop trigger no_delete on public.agg__ledger; \
         drop trigger no_delete on public.agg__deltas",
    )
    .await;
    fx.merge().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());

    let source = include_str!("../src/staging/partition_correction.rs");
    let code = source.split("#[cfg(test)]").next().expect("code");
    for forbidden in ["delete from", "truncate "] {
        assert!(
            !code.to_lowercase().contains(forbidden)
                || code
                    .lines()
                    .filter(|l| !l.trim_start().starts_with("//"))
                    .all(|l| !l.to_lowercase().contains(forbidden)),
            "the engine's code mentions {forbidden:?}"
        );
    }
}

/// Stopped after one page and run again: the second run starts after the
/// first page's cursor, and the keys re-derived across both are the key set.
#[tokio::test]
async fn a_correction_resumes_after_its_last_committed_page() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    fx.d.settle().await;
    let correction = fx.queue().await.remove(0);
    let definition =
        fx.d.ctl
            .query_one("select id from transform_definitions", &[])
            .await
            .expect("definition")
            .get::<_, i64>(0);

    let small = Options {
        page_keys: 7,
        max_pages: Some(2),
    };
    let first = fx.run(&small).await;
    assert_eq!(first.pages, 2);
    assert_eq!(first.keys, 14);
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
    assert_eq!(saved.phase(), "ledger");
    assert_eq!(saved.keys_done, 14);
    assert!(saved.cursor.is_some());
    assert_eq!(saved.keys_total, Some(40), "the leaf's 40 live entries");
    assert_eq!(saved.keys_remaining(), Some(26));

    // The restart: a new call, nothing but the committed progress.
    let rest = fx
        .run(&Options {
            page_keys: 7,
            max_pages: None,
        })
        .await;
    assert_eq!(rest.completed, 1);
    assert_eq!(rest.keys, 26, "it starts after the last committed page");
    assert!(fx.queue().await.is_empty());
    fx.merge().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// A detach that is still pending keeps writing through the parent into the
/// leaf for writers with older snapshots: the correction waits for the
/// `pg_inherits` row to go.
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
    let started: i64 =
        fx.d.ctl
            .query_one("select count(*) from partition_correction_progress", &[])
            .await
            .expect("progress rows")
            .get(0);
    assert_eq!(started, 0);

    holder.batch_execute("rollback").await.expect("release");
    fx.exec("alter table public.items detach partition public.items_p2 finalize")
        .await;
    fx.pass().await;
    fx.d.settle().await;
    let outcome = fx.run(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    fx.merge().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// Two aggregates read the source: the correction re-derives the key set for
/// each, and leaves the queue only when both have.
#[tokio::test]
async fn every_aggregate_reading_the_source_is_corrected_before_the_queue_clears() {
    const SECOND: &str =
        "TRANSFORM agg2 FROM public.items GROUP BY g SELECT MAX(v) AS top, COUNT(*) AS n";
    let mut fx = Fx::start_with(&tree("range"), &[DEFINITION, SECOND]).await;
    fx.exec("drop table public.items_p2").await;
    fx.pass().await;
    fx.d.settle().await;
    let outcome = fx.run(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    for target in ["agg", "agg2"] {
        let plan = fx.d.build_plan(target).await;
        fx.d.merge_all(&plan).await;
    }
    fx.d.settle().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
    let wrong: i64 =
        fx.d.ctl
            .query_one(
                "select count(*) from (select g, max(v) as top, count(*) as n \
                                   from public.items group by g) w \
             full join public.agg2 h on h.g = w.g \
             where w.top is distinct from h.top or w.n is distinct from h.n",
                &[],
            )
            .await
            .expect("oracle")
            .get(0);
    assert_eq!(wrong, 0, "the recomputed MAX of the second aggregate");
}

/// A second group column that can be `NULL` pages in `(group, key)` order,
/// `NULL` last, in pages smaller than a group.
#[tokio::test]
async fn a_nullable_second_group_column_pages_in_order_and_corrects_every_group() {
    const GROUPED: &str = "TRANSFORM agg FROM public.items GROUP BY g, h \
         SELECT SUM(v) AS total, COUNT(*) AS n";
    let mut fx = Fx::start_with(
        "create table public.items (id integer not null, g integer not null, h integer, \
             v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_p0 partition of public.items for values from (0) to (8); \
         create table public.items_p1 partition of public.items for values from (8) to (16); \
         insert into public.items select i, i % 16, case when i % 3 = 0 then null else i % 4 end, \
             i from generate_series(1, 400) i;",
        &[GROUPED],
    )
    .await;
    fx.exec("drop table public.items_p0").await;
    fx.pass().await;
    fx.d.settle().await;
    let outcome = fx
        .run(&Options {
            page_keys: 5,
            max_pages: None,
        })
        .await;
    assert_eq!(outcome.completed, 1);
    assert!(outcome.pages > 10, "{} pages of five keys", outcome.pages);
    fx.merge().await;
    let wrong: i64 =
        fx.d.ctl
            .query_one(
                "select count(*) from (select g, h, sum(v) as total, count(*) as n \
                                   from public.items group by g, h) w \
             full join public.agg h on h.g = w.g and h.h is not distinct from w.h \
             where w.total is distinct from h.total or w.n is distinct from h.n",
                &[],
            )
            .await
            .expect("oracle")
            .get(0);
    assert_eq!(wrong, 0);
}

/// The node phase pages too: an attached leaf read five keys at a time, one
/// page per call, finishes with the same counts as one call.
#[tokio::test]
async fn the_node_phase_resumes_from_its_cursor() {
    let mut fx = Fx::start(&tree("list")).await;
    fx.exec("alter table public.items detach partition public.items_p3")
        .await;
    fx.correct().await;
    fx.exec("update public.items_p3 set v = v + 5000").await;
    fx.exec(
        "alter table public.items attach partition public.items_p3 for values in (12, 13, 14, 15)",
    )
    .await;
    fx.pass().await;
    fx.d.settle().await;
    let correction = fx.queue().await.remove(0);
    let definition: i64 =
        fx.d.ctl
            .query_one("select id from transform_definitions", &[])
            .await
            .expect("definition")
            .get(0);
    let mut seen_node_cursor = false;
    let mut total = 0;
    for _ in 0..200 {
        let outcome = fx
            .run(&Options {
                page_keys: 5,
                max_pages: Some(1),
            })
            .await;
        total += outcome.keys;
        if outcome.completed == 1 {
            break;
        }
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
    // The detach's correction retracted the leaf's groups' entries, so no live
    // entry routes to it: its 40 rows are the key set.
    assert_eq!(total, 40);
    fx.merge().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// `--ignored`: what a retention `DROP` of a leaf of one million rows costs
/// (ADR-0017 reports the figures). The leaf holds 1,000,000
/// rows over 50,000 groups; its three siblings 10,000 each. Prints the time
/// the engine takes, the time the merger takes to fold its deltas, and the
/// WAL both wrote.
#[tokio::test]
#[ignore = "a measurement: run with logbook reserve and a memory cap"]
async fn measure_a_correction_of_a_million_row_leaf() {
    let mut fx = Fx::start(
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_p0 partition of public.items for values from (0) to (50000); \
         create table public.items_p1 partition of public.items \
             for values from (50000) to (100000); \
         create table public.items_p2 partition of public.items \
             for values from (100000) to (150000); \
         create table public.items_p3 partition of public.items \
             for values from (150000) to (200000); \
         insert into public.items select i, 50000 + i % 50000, i % 97 \
             from generate_series(1, 1000000) i; \
         insert into public.items select 2000000 + i, (i * 7) % 50000, 1 \
             from generate_series(1, 10000) i; \
         insert into public.items select 3000000 + i, 100000 + (i * 7) % 50000, 1 \
             from generate_series(1, 10000) i; \
         insert into public.items select 4000000 + i, 150000 + (i * 7) % 50000, 1 \
             from generate_series(1, 10000) i; \
         analyze public.items;",
    )
    .await;
    assert!(fx.wrong_groups().await.is_empty());
    async fn wal(fx: &Fx) -> String {
        fx.d.ctl
            .query_one("select pg_current_wal_lsn()::text", &[])
            .await
            .expect("lsn")
            .get::<_, String>(0)
    }
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    fx.d.settle().await;
    let before = wal(&fx).await;
    let started = Instant::now();
    let outcome = fx.run(&Options::default()).await;
    let engine = started.elapsed();
    let middle = wal(&fx).await;
    let started = Instant::now();
    fx.merge().await;
    let merge = started.elapsed();
    let after = wal(&fx).await;
    let bytes = |from: &str, to: &str| -> i64 {
        // Both are `X/Y`: the difference of the 64-bit positions.
        let parse = |lsn: &str| {
            let (hi, lo) = lsn.split_once('/').expect("lsn");
            (i64::from_str_radix(hi, 16).unwrap() << 32) + i64::from_str_radix(lo, 16).unwrap()
        };
        parse(to) - parse(from)
    };
    eprintln!(
        "MEASURE leaf_rows=1000000 keys={} pages={} engine_s={:.2} engine_wal_mb={:.0} \
         merge_s={:.2} merge_wal_mb={:.0}",
        outcome.keys,
        outcome.pages,
        engine.as_secs_f64(),
        bytes(&before, &middle) as f64 / 1e6,
        merge.as_secs_f64(),
        bytes(&middle, &after) as f64 / 1e6,
    );
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// A `HASH` leaf's text embeds its parent's oid. A middle dropped before the
/// leaf's correction has run takes that oid with it, and the middle's own
/// correction routes every group of its bound, the leaf's among them.
#[tokio::test]
async fn a_gone_hash_leaf_whose_middle_was_dropped_after_it_is_covered_by_the_middles_correction() {
    const GROUPED: &str = "TRANSFORM agg FROM public.items GROUP BY g, h \
         SELECT SUM(v) AS total, COUNT(*) AS n";
    let mut fx = Fx::start_with(
        "create table public.items (id integer not null, g integer not null, \
             h smallint not null, v integer not null, primary key (g, h, id)) \
             partition by range (g); \
         create table public.items_m0 partition of public.items for values from (0) to (8) \
             partition by hash (h); \
         create table public.items_m0a partition of public.items_m0 \
             for values with (modulus 2, remainder 0); \
         create table public.items_m0b partition of public.items_m0 \
             for values with (modulus 2, remainder 1); \
         create table public.items_m1 partition of public.items for values from (8) to (16); \
         insert into public.items select i, i % 16, (i % 7)::smallint, i \
             from generate_series(1, 320) i;",
        &[GROUPED],
    )
    .await;
    fx.exec("drop table public.items_m0a").await;
    fx.pass().await;
    // Before the leaf's correction runs, its middle goes too.
    fx.exec("drop table public.items_m0").await;
    fx.pass().await;
    fx.d.settle().await;
    let queue = fx.queue().await;
    assert_eq!(
        queued(&queue),
        [
            entry("gone_node", "public.items_m0a"),
            entry("gone_node", "public.items_m0")
        ]
    );
    let outcome = fx.run(&Options::default()).await;
    assert_eq!(outcome.completed, 2);
    fx.merge().await;
    let wrong: i64 =
        fx.d.ctl
            .query_one(
                "select count(*) from (select g, h, sum(v) as total, count(*) as n \
                                   from public.items group by g, h) w \
             full join public.agg a on a.g = w.g and a.h = w.h \
             where w.total is distinct from a.total or w.n is distinct from a.n",
                &[],
            )
            .await
            .expect("oracle")
            .get(0);
    assert_eq!(wrong, 0);
}

/// Review of #1062: a `catching_up` definition applies as a `live` one does,
/// so a correction re-derives its keys too, instead of completing without it.
#[tokio::test]
async fn a_catching_up_aggregate_is_corrected() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("update transform_definitions set status = 'catching_up'")
        .await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    fx.d.settle().await;
    let outcome = fx.run(&Options::default()).await;
    assert_eq!(outcome.completed, 1);
    assert_eq!(
        outcome.keys, 40,
        "the dropped leaf's entries are re-derived"
    );
    fx.merge().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// Review of #1062: a detached leaf attached again before its correction ran
/// has a `pg_inherits` row that is not detach-pending. Its correction runs
/// (each key it re-derives reads the source as it is) rather than waiting
/// for a detach that is not happening.
#[tokio::test]
async fn a_leaf_attached_again_before_its_correction_ran_does_not_hold_the_queue() {
    for kind in ["hash", "range", "list"] {
        let mut fx = Fx::start(&tree(kind)).await;
        fx.exec("alter table public.items detach partition public.items_p2")
            .await;
        fx.pass().await;
        fx.exec("update public.items_p2 set v = v + 1000").await;
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
        let queue = queued(&fx.queue().await);
        assert_eq!(
            queue,
            [
                entry("gone_node", "public.items_p2"),
                entry("new_leaf", "public.items_p2")
            ],
            "{kind}"
        );
        let outcome = fx.run(&Options::default()).await;
        assert_eq!(outcome.completed, 2, "{kind}");
        fx.merge().await;
        assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{kind}");
    }
}
