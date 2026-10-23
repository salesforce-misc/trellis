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

use std::time::{Duration, Instant, SystemTime};

use drain_driver::Driver;
use trellis::capture::partitions::{self, PendingCorrection};
use trellis::capture::reconcile::PassOutcome;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::interleave::{PausePoint, PauseScope};
use trellis::staging::partition_correction::{self, Options, Outcome};
use trellis::{Config, CorrectingNode, TransformStatus, Trellis, TrellisOptions};

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
        partition_correction::run_pending(self.d.pool(), options)
            .await
            .expect("run the pending corrections")
    }

    async fn merge(&mut self) {
        let plan = self.d.build_plan("agg").await;
        self.d.merge_all(&plan).await;
        self.d.settle().await;
    }

    /// The definition's status.
    async fn status(&self) -> String {
        self.status_of("agg").await
    }

    /// The status of the definition whose bare target is `target`.
    async fn status_of(&self, target: &str) -> String {
        self.d
            .ctl
            .query_one(
                "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
                &[&target],
            )
            .await
            .expect("status")
            .get(0)
    }

    /// One whole staging-worker pass, as the maintenance loop runs it:
    /// capture, then the corrections' rows (the status flip), then builds and
    /// markers.
    async fn full_pass(&mut self) {
        let pool = self.d.pool().clone();
        trellis::client::reconcile_pass(
            &mut self.d.ctl,
            &pool,
            DEFAULT_SCHEMA,
            drain_driver::WAKE,
            Duration::from_secs(2),
        )
        .await
        .expect("reconcile pass");
    }

    /// The drain workers' build work, run in this task until no build is
    /// running: the correction rows, the merges of their deltas, and the
    /// completion that moves a definition back to `live`.
    async fn settle_builds(&mut self) {
        self.d.settle().await;
        build::settle_builds(self.d.pool()).await;
    }

    /// Claims and runs every claimable correction row once, as a drain worker
    /// does: a row that fails is given up through the chunk queue's
    /// `fail_chunk`, which records the error and backs the row off.
    async fn worker_turns(&self) {
        let options = WorkerOptions {
            chunk_rows: build::DEFAULT_CHUNK_ROWS,
            drain_batch_cap: usize::MAX,
            heartbeat_interval: Duration::from_secs(5),
            reclaim_ttl: Duration::from_secs(60),
        };
        loop {
            let claimed = {
                let client = self.d.pool().get().await.expect("pool");
                chunk_queue::claim_chunks_of(
                    &**client,
                    "worker",
                    1,
                    &[chunk_queue::KIND_CORRECTION],
                )
                .await
                .expect("claim")
            };
            let Some(chunk) = claimed.into_iter().next() else {
                return;
            };
            build::run_claimed(self.d.pool(), &chunk, "worker", &options).await;
        }
    }

    /// The correction rows, as `(target, done, attempts, last_error)`.
    async fn rows(&self) -> Vec<(String, bool, i32, Option<String>)> {
        self.d
            .ctl
            .query(
                "select split_part(d.target_table, '.', 2), bc.done, bc.attempts, bc.last_error \
                 from backfill_chunks bc join transform_definitions d on d.id = bc.definition_id \
                 where bc.kind = 'correction' order by bc.id",
                &[],
            )
            .await
            .expect("rows")
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
            .collect()
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

/// A page's Re-derive reads each key's source row once, whether the rows
/// are all there (an attached leaf's keys, the node phase's) or none is (a
/// dropped leaf's), #1085. Joining the keys to the source and then the keys
/// to that join, the planner estimated the first join at one row and put it
/// on the inner side of a nested loop over the keys: materialized and
/// compared with every key (16.8 s for a page of 10,000 present keys), or
/// rescanned per key. The check is on `explain analyze`'s actual counts, so
/// it is deterministic: no node handles more rows over all its loops than
/// twice the page.
#[tokio::test]
async fn a_rederive_page_reads_each_key_once_whether_or_not_its_row_is_there() {
    const PAGE: usize = 2_000;
    let mut over = Vec::new();
    for op in ["present", "dropped"] {
        let fx = Fx::start(
            "create table public.items (id integer not null, g integer not null, \
                 v integer not null, primary key (g, id)) partition by range (g); \
             create table public.items_p0 partition of public.items for values from (0) to (10000); \
             create table public.items_p1 partition of public.items \
                 for values from (10000) to (20000); \
             create table public.items_p2 partition of public.items \
                 for values from (20000) to (30000); \
             insert into public.items select i, i % 30000, i from generate_series(1, 60000) i; \
             analyze public.items;",
        )
        .await;
        if op == "dropped" {
            fx.exec("drop table public.items_p1").await;
        }
        let keys: Vec<String> =
            fx.d.ctl
                .query(
                    "select __from_key from public.agg__ledger \
                     where g >= 10000 and g < 20000 order by g, __from_key limit $1",
                    &[&i64::try_from(PAGE).expect("a page")],
                )
                .await
                .expect("the leaf's keys")
                .iter()
                .map(|r| r.get(0))
                .collect();
        assert_eq!(keys.len(), PAGE);
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        let plan = fx.d.build_plan("agg").await;
        let mut client = fx.d.pool().get().await.expect("pool");
        let txn = client.transaction().await.expect("begin");
        let plans = build::explain_sweep(&txn, &plan, &keys, "analyze, timing off, buffers off")
            .await
            .expect("explain the Re-derive");
        txn.rollback().await.expect("roll back");
        for explained in plans {
            if explained.lines().any(|line| handled_rows(line) > 2 * PAGE) {
                over.push(format!("{op}:\n{explained}"));
            }
        }
    }
    assert!(
        over.is_empty(),
        "a node handled more than twice the page:\n{}",
        over.join("\n")
    );
}

/// The rows an `explain analyze` line's node handled over all its loops: its
/// actual `rows=` times its `loops=`.
fn handled_rows(line: &str) -> usize {
    let Some(actual) = line.split("(actual ").nth(1) else {
        return 0;
    };
    let figure = |name: &str| -> usize {
        actual
            .split(name)
            .nth(1)
            .and_then(|rest| rest.split([' ', ')']).next())
            .and_then(|n| n.parse::<f64>().ok())
            .map_or(0, |n| n as usize)
    };
    figure("rows=") * figure("loops=")
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

/// A new `HASH` leaf's correction routes by the leaf's text, which embeds its
/// middle's oid. A middle dropped before the correction has run makes that
/// text raise `XX000`; the correction is complete without work instead
/// (#1148), and the middle's own `gone_node`, queued by the next pass,
/// corrects the region.
#[tokio::test]
async fn a_new_hash_leaf_whose_middle_was_dropped_completes_without_error() {
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
    // The remainder-1 leaf is dropped and made again: a new leaf, whose
    // text names the middle, over groups that hold rows in the remainder 0.
    fx.exec("drop table public.items_m0b").await;
    fx.pass().await;
    fx.exec(
        "create table public.items_m0b partition of public.items_m0 \
         for values with (modulus 2, remainder 1)",
    )
    .await;
    fx.pass().await;
    fx.d.settle().await;
    assert_eq!(
        queued(&fx.queue().await),
        [
            entry("gone_node", "public.items_m0b"),
            entry("new_leaf", "public.items_m0b")
        ]
    );
    // Before the leaf's correction runs, its middle goes too.
    fx.exec("drop table public.items_m0").await;
    let outcome = fx.run(&Options::default()).await;
    assert_eq!(outcome.completed, 2);
    assert!(fx.queue().await.is_empty());
    fx.correct().await;
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

/// A new `HASH` leaf moved out from under its middle, and the middle dropped,
/// before the leaf's correction ran: the leaf's rows are still in the source,
/// under a parent the recorded tree does not know yet. The correction waits
/// for the next pass to record the leaf where it is now rather than
/// completing without work: that pass's `new_leaf` of the leaf is the same
/// queue entry while another reader (here a paused one) keeps it queued, so
/// nothing else would count the leaf's rows for this aggregate.
#[tokio::test]
async fn a_new_hash_leaf_moved_from_under_its_dropped_middle_is_still_counted() {
    for reattach in [true, false] {
        a_new_hash_leaf_moved_from_under_its_dropped_middle(reattach).await;
    }
}

/// [`a_new_hash_leaf_moved_from_under_its_dropped_middle_is_still_counted`]:
/// the leaf is attached to the root again, or left out of the tree.
async fn a_new_hash_leaf_moved_from_under_its_dropped_middle(reattach: bool) {
    const GROUPED: &str = "TRANSFORM agg FROM public.items GROUP BY g, h \
         SELECT SUM(v) AS total, COUNT(*) AS n";
    const SECOND: &str = "TRANSFORM agg2 FROM public.items GROUP BY g, h \
         SELECT MAX(v) AS top, COUNT(*) AS n";
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
        &[GROUPED, SECOND],
    )
    .await;
    fx.exec("drop table public.items_m0b").await;
    fx.correct().await;
    // A table with rows in groups no other leaf holds is attached in the
    // dropped leaf's place.
    fx.exec(
        "create table public.items_x (id integer not null, g integer not null, \
             h smallint not null, v integer not null, primary key (g, h, id)); \
         insert into public.items_x select i, i % 8, (100 + i % 10)::smallint, i \
             from generate_series(1000, 1099) i \
             where satisfies_hash_partition('public.items_m0'::regclass, 2, 1, \
                                            (100 + i % 10)::smallint); \
         alter table public.items_m0 attach partition public.items_x \
             for values with (modulus 2, remainder 1)",
    )
    .await;
    fx.pass().await;
    fx.d.settle().await;
    assert_eq!(
        queued(&fx.queue().await),
        [entry("new_leaf", "public.items_x")]
    );
    // Both readers get their row; the second is paused, so its row keeps
    // the correction queued.
    partition_correction::schedule(fx.d.pool())
        .await
        .expect("schedule");
    fx.exec("update transform_definitions set status = 'paused' where target_table like '%.agg2'")
        .await;
    // The leaf moves out, to the root or out of the tree, and its middle
    // goes, before the next pass.
    fx.exec(
        "alter table public.items_m0 detach partition public.items_x; \
         drop table public.items_m0",
    )
    .await;
    if reattach {
        fx.exec(
            "alter table public.items attach partition public.items_x \
             for values from (0) to (8)",
        )
        .await;
    }
    // The leaf's correction waits for the next pass's record of the tree.
    let outcome = fx.run(&Options::default()).await;
    assert_eq!((outcome.completed, outcome.remaining), (0, 1), "{reattach}");
    fx.correct().await;
    assert_eq!(
        queued(&fx.queue().await),
        [entry("new_leaf", "public.items_x")],
        "{reattach}: only the paused reader's row keeps it"
    );
    let wrong: Vec<String> =
        fx.d.ctl
            .query(
                "select coalesce(w.g, a.g)::text || '/' || coalesce(w.h, a.h)::text \
                 from (select g, h, sum(v) as total, count(*) as n \
                       from public.items group by g, h) w \
                 full join public.agg a on a.g = w.g and a.h = w.h \
                 where w.total is distinct from a.total or w.n is distinct from a.n \
                 order by 1",
                &[],
            )
            .await
            .expect("oracle")
            .iter()
            .map(|r| r.get(0))
            .collect();
    assert_eq!(wrong, Vec::<String>::new(), "{reattach}");
}

/// A `catching_up` definition applies, so the correction waits for it: it
/// gets its row once its catch-up has moved it `live` (the merger folds only
/// a `backfilling` definition's deltas), and the correction stays queued
/// until then rather than completing without it.
#[tokio::test]
async fn a_catching_up_aggregate_holds_the_correction_until_it_is_live() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("update transform_definitions set status = 'catching_up'")
        .await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    fx.d.settle().await;
    let waiting = fx.run(&Options::default()).await;
    assert_eq!(waiting.completed, 0);
    assert_eq!(waiting.remaining, 1);
    assert_eq!(fx.status().await, "catching_up");
    fx.exec("update transform_definitions set status = 'live'")
        .await;
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

const SECOND_AGG: &str =
    "TRANSFORM agg2 FROM public.items GROUP BY g SELECT MAX(v) AS top, COUNT(*) AS n";

const RANGE_P2: &str = "alter table public.items attach partition public.items_p2 \
     for values from (8) to (12)";

/// A re-attach detected while an older `new_leaf` of the leaf is held by a
/// paused reader queues its own entry, so the reader that finished the older
/// one corrects the later operation too (#1151).
#[tokio::test]
async fn a_new_leaf_detected_while_an_older_one_is_held_by_a_paused_reader_is_queued_again() {
    let mut fx = Fx::start_with(&tree("range"), &[DEFINITION, SECOND_AGG]).await;
    fx.exec("alter table public.items detach partition public.items_p2")
        .await;
    fx.correct().await;
    fx.exec(RANGE_P2).await;
    fx.pass().await;
    fx.d.settle().await;
    partition_correction::schedule(fx.d.pool())
        .await
        .expect("schedule");
    fx.exec("update transform_definitions set status = 'paused' where target_table like '%.agg2'")
        .await;
    fx.correct().await;
    assert_eq!(
        queued(&fx.queue().await),
        [entry("new_leaf", "public.items_p2")],
        "agg2's undone row holds the entry"
    );
    fx.exec("alter table public.items detach partition public.items_p2")
        .await;
    fx.exec("update public.items_p2 set v = v + 1000").await;
    fx.exec(RANGE_P2).await;
    fx.correct().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// The same for a `gone_node`: detach, correct, attach, correct, then detach
/// again while the first `gone_node` is still held. The reader that finished
/// the first corrects the second.
#[tokio::test]
async fn a_gone_node_detected_while_an_older_one_is_held_by_a_paused_reader_is_queued_again() {
    let mut fx = Fx::start_with(&tree("range"), &[DEFINITION, SECOND_AGG]).await;
    fx.exec("alter table public.items detach partition public.items_p2")
        .await;
    fx.pass().await;
    fx.d.settle().await;
    partition_correction::schedule(fx.d.pool())
        .await
        .expect("schedule");
    fx.exec("update transform_definitions set status = 'paused' where target_table like '%.agg2'")
        .await;
    fx.correct().await;
    fx.exec(RANGE_P2).await;
    fx.correct().await;
    fx.exec("update public.items_p2 set v = v + 1000").await;
    fx.exec("alter table public.items detach partition public.items_p2")
        .await;
    fx.correct().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// A reader part-way through a correction may have walked past rows a later
/// operation changed, so the later operation queues a fresh entry.
#[tokio::test]
async fn an_operation_on_a_node_whose_correction_is_part_way_queues_a_fresh_entry() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("alter table public.items detach partition public.items_p2")
        .await;
    fx.correct().await;
    fx.exec(RANGE_P2).await;
    fx.pass().await;
    fx.d.settle().await;
    let part = fx
        .run(&Options {
            page_keys: 7,
            max_pages: Some(2),
        })
        .await;
    assert_eq!((part.pages, part.remaining), (2, 1));
    fx.exec("alter table public.items detach partition public.items_p2")
        .await;
    fx.exec("update public.items_p2 set v = v + 1000").await;
    fx.exec(RANGE_P2).await;
    fx.pass().await;
    assert_eq!(
        queued(&fx.queue().await),
        [
            entry("new_leaf", "public.items_p2"),
            entry("new_leaf", "public.items_p2")
        ]
    );
    fx.d.settle().await;
    fx.run(&Options::default()).await;
    fx.merge().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// Detections of the same node and text while no reader has started the
/// queued entry add nothing.
#[tokio::test]
async fn repeated_detections_while_no_reader_has_started_queue_one_entry() {
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("alter table public.items detach partition public.items_p2")
        .await;
    fx.pass().await;
    fx.exec(RANGE_P2).await;
    fx.pass().await;
    fx.exec("alter table public.items detach partition public.items_p2")
        .await;
    fx.pass().await;
    fx.exec(RANGE_P2).await;
    fx.pass().await;
    assert_eq!(
        queued(&fx.queue().await),
        [
            entry("gone_node", "public.items_p2"),
            entry("new_leaf", "public.items_p2")
        ]
    );
}

/// `items` partitioned `by` on `g` into two middles, each partitioned again,
/// with the same 160 rows as [`tree`]. `items_m0` holds half the groups.
fn nested(kind: &str) -> String {
    let head = format!(
        "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by {kind} (g);"
    );
    let (m0, m1, sub) = match kind {
        "hash" => (
            "for values with (modulus 2, remainder 0)",
            "for values with (modulus 2, remainder 1)",
            ["for values from (0) to (8)", "for values from (8) to (16)"],
        ),
        "range" => (
            "for values from (0) to (8)",
            "for values from (8) to (16)",
            [
                "for values with (modulus 2, remainder 0)",
                "for values with (modulus 2, remainder 1)",
            ],
        ),
        _ => (
            "for values in (0, 1, 2, 3, 4, 5, 6, 7)",
            "for values in (8, 9, 10, 11, 12, 13, 14, 15)",
            [
                "for values with (modulus 2, remainder 0)",
                "for values with (modulus 2, remainder 1)",
            ],
        ),
    };
    let by = if kind == "hash" { "range" } else { "hash" };
    let mut ddl = head;
    for (middle, bound) in [("items_m0", m0), ("items_m1", m1)] {
        ddl.push_str(&format!(
            "create table public.{middle} partition of public.items {bound} partition by {by} (g);"
        ));
        for (leaf, sub) in ["a", "b"].iter().zip(sub) {
            ddl.push_str(&format!(
                "create table public.{middle}{leaf} partition of public.{middle} {sub};"
            ));
        }
    }
    ddl.push_str("insert into public.items select i, i % 16, i from generate_series(1, 160) i;");
    ddl
}

/// Through the staging worker's own path, for each lifecycle operation on
/// each partitioning: the pass that queues the correction moves the aggregate
/// to `backfilling` (its groups are wrong until the correction drains), the
/// drain workers' build work runs the correction's row and folds its deltas,
/// and the definition is `live` again, with no group wrong and nothing left
/// in the queue.
#[tokio::test]
async fn every_lifecycle_operation_reports_backfilling_until_its_correction_drains() {
    for kind in ["hash", "range", "list"] {
        for op in ["attach", "detach", "drop", "new leaf", "middle drop"] {
            let ddl = if op == "middle drop" {
                nested(kind)
            } else {
                tree(kind)
            };
            let mut fx = Fx::start(&ddl).await;
            assert_eq!(fx.status().await, "live", "{kind} {op}");
            match op {
                "attach" => {
                    fx.exec("alter table public.items detach partition public.items_p2")
                        .await;
                    fx.full_pass().await;
                    fx.settle_builds().await;
                    assert_eq!(fx.status().await, "live", "{kind} {op}: the detach drained");
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
                }
                "detach" => {
                    fx.exec("alter table public.items detach partition public.items_p1")
                        .await;
                }
                "drop" => fx.exec("drop table public.items_p1").await,
                "new leaf" => {
                    let clause = match kind {
                        "hash" => "for values with (modulus 4, remainder 3)",
                        "range" => "for values from (12) to (16)",
                        _ => "for values in (12, 13, 14, 15)",
                    };
                    let group: i32 =
                        fx.d.ctl
                            .query_one("select g from public.items_p3 limit 1", &[])
                            .await
                            .expect("a group of the leaf")
                            .get(0);
                    fx.exec("drop table public.items_p3").await;
                    fx.exec(&format!(
                        "create table public.items_new partition of public.items {clause}; \
                         insert into public.items_new values (1001, {group}, 7), (1002, {group}, 9)"
                    ))
                    .await;
                }
                _ => fx.exec("drop table public.items_m0").await,
            }
            fx.full_pass().await;
            assert!(!fx.queue().await.is_empty(), "{kind} {op}");
            assert_eq!(
                fx.status().await,
                "backfilling",
                "{kind} {op}: from the pass that queued the correction"
            );
            fx.d.settle().await;
            assert!(
                !fx.wrong_groups().await.is_empty(),
                "{kind} {op}: wrong until the correction drains"
            );
            fx.settle_builds().await;
            assert_eq!(fx.status().await, "live", "{kind} {op}");
            assert!(fx.queue().await.is_empty(), "{kind} {op}");
            assert!(
                fx.rows().await.is_empty(),
                "{kind} {op}: the rows went with the correction"
            );
            assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{kind} {op}");
        }
    }
}

/// A worker that dies before the last page of a correction commits: the page
/// rolls back, the stale-claim sweep frees the row, and the next claim resumes
/// after the last committed page. The definition reads `backfilling`
/// throughout, and `live` once the correction has drained.
#[tokio::test]
async fn a_worker_that_dies_before_the_last_page_commits_is_resumed_from_its_progress() {
    const KEY: i64 = 1_062_002;
    let mut fx = Fx::start(&tree("range")).await;
    fx.exec("drop table public.items_p1").await;
    fx.full_pass().await;
    fx.d.settle().await;
    assert_eq!(fx.status().await, "backfilling");
    let correction = fx.queue().await.remove(0);
    let (definition, target): (i64, String) = {
        let row =
            fx.d.ctl
                .query_one("select id, target_table from transform_definitions", &[])
                .await
                .expect("definition");
        (row.get(0), row.get(1))
    };
    // The leaf's 40 live entries in pages of seven: five pages commit.
    let first = fx
        .run(&Options {
            page_keys: 7,
            max_pages: Some(5),
        })
        .await;
    assert_eq!((first.pages, first.keys, first.remaining), (5, 35, 1));

    // The worker that claims it next stops before the sixth, last page
    // commits, and dies there.
    let holder = drain_driver::connect(fx.d.db.dsn()).await;
    holder
        .execute("select pg_advisory_lock($1)", &[&KEY])
        .await
        .expect("hold the pause");
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::BeforeCorrectionCommit, &target, KEY);
    let pool = fx.d.pool().clone();
    let worker = tokio::spawn(trellis::staging::interleave::with_scope(
        scope,
        async move {
            partition_correction::run_pending(
                &pool,
                &Options {
                    page_keys: 7,
                    max_pages: None,
                },
            )
            .await
        },
    ));
    let at = reached.await.expect("the last page reached its commit");
    worker.abort();
    // The two-argument form waits server-side until the backend has exited:
    // its locks are released (the reclaim sweep skips locked rows, #1124) and
    // its socket is closed.
    let ended: bool =
        fx.d.ctl
            .query_one("select pg_terminate_backend($1, 10000)", &[&at.backend_pid])
            .await
            .expect("end the worker's session")
            .get(0);
    assert!(ended, "the worker's backend exited");
    holder
        .execute("select pg_advisory_unlock($1)", &[&KEY])
        .await
        .expect("release");

    let saved = partition_correction::progress(&fx.d.ctl, correction.id, definition)
        .await
        .expect("progress")
        .expect("committed progress");
    assert_eq!(
        (saved.phase(), saved.keys_done),
        ("ledger", 35),
        "the dead page's progress rolled back with its keys"
    );
    assert_eq!(fx.status().await, "backfilling");
    let claimed: Option<String> =
        fx.d.ctl
            .query_one(
                "select claimed_by from backfill_chunks where kind = 'correction'",
                &[],
            )
            .await
            .expect("the row")
            .get(0);
    assert!(claimed.is_some(), "the dead worker's claim is still there");
    let freed = chunk_queue::reclaim_stale_chunks(&mut fx.d.ctl, Duration::ZERO)
        .await
        .expect("reclaim");
    assert_eq!(freed, 1);

    // The aborted worker's pooled connection is dead. Everything from here
    // on runs on a pool built now, which holds none of it, so no call can be
    // handed a closed connection (#1152).
    fx.d.db.pool =
        trellis::Pool::new(&Config::from_dsn(fx.d.db.dsn().to_string()).expect("valid dsn"))
            .expect("a fresh pool");
    let rest = fx
        .run(&Options {
            page_keys: 7,
            max_pages: None,
        })
        .await;
    assert_eq!(rest.completed, 1);
    assert_eq!(rest.keys, 5, "only the page that did not commit runs again");
    fx.settle_builds().await;
    assert_eq!(fx.status().await, "live");
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// `Trellis::status` reports a definition under correction as `backfilling`,
/// with a `correcting` entry for the node: its schema-qualified name, when
/// the pass queued the correction, and the keys left by the persisted
/// progress. A page that has not committed doesn't count. Once the last page
/// commits the entry leaves, and once the deltas are folded the definition is
/// `live` with nothing listed.
#[tokio::test]
async fn status_lists_the_node_under_correction_until_its_keys_drain() {
    const KEY: i64 = 1_065_001;
    let mut fx = Fx::start(&tree("range")).await;
    let handle = Trellis::connect(
        Config::from_dsn(fx.d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    let status = handle.status("agg").await.expect("status").expect("agg");
    assert_eq!(
        (status.status, status.correcting),
        (TransformStatus::Live, Vec::new())
    );

    fx.exec("drop table public.items_p1").await;
    fx.full_pass().await;
    fx.d.settle().await;
    let queued_at: SystemTime =
        fx.d.ctl
            .query_one("select queued_at from partition_corrections", &[])
            .await
            .expect("the correction")
            .get(0);
    let node = |keys_remaining| CorrectingNode {
        node: "public.items_p1".to_string(),
        since: queued_at,
        keys_remaining,
    };
    let status = handle.status("agg").await.expect("status").expect("agg");
    assert_eq!(status.status, TransformStatus::Backfilling);
    assert_eq!(
        status.correcting,
        vec![node(None)],
        "listed from the pass that queued it, uncounted before its first page"
    );

    // The leaf's 40 live entries in pages of seven: five pages commit.
    let first = fx
        .run(&Options {
            page_keys: 7,
            max_pages: Some(5),
        })
        .await;
    assert_eq!((first.pages, first.keys, first.remaining), (5, 35, 1));
    let status = handle.status("agg").await.expect("status").expect("agg");
    assert_eq!(status.correcting, vec![node(Some(5))]);

    // The next claim stops before its last page commits.
    let holder = drain_driver::connect(fx.d.db.dsn()).await;
    holder
        .execute("select pg_advisory_lock($1)", &[&KEY])
        .await
        .expect("hold the pause");
    let target: String =
        fx.d.ctl
            .query_one("select target_table from transform_definitions", &[])
            .await
            .expect("definition")
            .get(0);
    let scope = PauseScope::new();
    let reached = scope.arm(PausePoint::BeforeCorrectionCommit, &target, KEY);
    let pool = fx.d.pool().clone();
    let worker = tokio::spawn(trellis::staging::interleave::with_scope(
        scope,
        async move {
            partition_correction::run_pending(
                &pool,
                &Options {
                    page_keys: 7,
                    max_pages: None,
                },
            )
            .await
        },
    ));
    reached.await.expect("the last page reached its commit");
    let status = handle.status("agg").await.expect("status").expect("agg");
    assert_eq!(
        (status.status, status.correcting),
        (TransformStatus::Backfilling, vec![node(Some(5))]),
        "the page that has not committed doesn't count"
    );
    holder
        .execute("select pg_advisory_unlock($1)", &[&KEY])
        .await
        .expect("release");
    let rest = worker
        .await
        .expect("the worker")
        .expect("run the pending corrections");
    assert_eq!(rest.completed, 1);
    assert_eq!(
        handle
            .status("agg")
            .await
            .expect("status")
            .expect("agg")
            .correcting,
        Vec::new(),
        "its keys are re-derived"
    );

    fx.settle_builds().await;
    let status = handle.status("agg").await.expect("status").expect("agg");
    assert_eq!(
        (status.status, status.correcting),
        (TransformStatus::Live, Vec::new())
    );
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
    handle.shutdown().await.expect("shut down");
}

/// One aggregate's correction keeps failing (its ledger refuses the
/// tombstones the correction writes). The other aggregate reading the source
/// is corrected and goes `live`. The failing row records its error and backs
/// off, and its charged failures pause its definition; the correction then
/// stays queued with the row, the record of why, until a resume.
#[tokio::test]
async fn a_failing_correction_holds_up_no_other_definition_and_pauses_its_own() {
    const SECOND: &str =
        "TRANSFORM agg2 FROM public.items GROUP BY g SELECT MAX(v) AS top, COUNT(*) AS n";
    let mut fx = Fx::start_with(&tree("range"), &[DEFINITION, SECOND]).await;
    let ledger: String =
        fx.d.ctl
            .query_one(
                "select format('%I.%I', schemaname, tablename) from pg_tables \
                 where tablename = 'agg__ledger'",
                &[],
            )
            .await
            .expect("the ledger")
            .get(0);
    fx.exec(&format!(
        "alter table {ledger} add constraint no_tombstones check (not __tombstone) not valid"
    ))
    .await;
    fx.exec("drop table public.items_p1").await;
    fx.full_pass().await;
    fx.d.settle().await;
    assert_eq!(fx.status_of("agg").await, "backfilling");
    assert_eq!(fx.status_of("agg2").await, "backfilling");
    let handle = Trellis::connect(
        Config::from_dsn(fx.d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    let correcting = async |target: &str| -> Vec<String> {
        let status = handle.status(target).await.expect("status").expect(target);
        status.correcting.into_iter().map(|c| c.node).collect()
    };

    fx.worker_turns().await;
    assert_eq!(
        correcting("agg").await,
        vec!["public.items_p1".to_string()],
        "its failing row is a correction still in progress"
    );
    assert_eq!(
        correcting("agg2").await,
        Vec::<String>::new(),
        "its keys are re-derived"
    );
    let rows = fx.rows().await;
    assert_eq!(rows.len(), 2);
    let failing = rows.iter().find(|r| r.0 == "agg").expect("agg's row");
    assert!(!failing.1 && failing.2 == 1, "{failing:?}");
    assert!(
        failing
            .3
            .as_deref()
            .is_some_and(|e| e.contains("no_tombstones")),
        "{failing:?}"
    );
    assert!(
        rows.iter().any(|r| r.0 == "agg2" && r.1),
        "the other aggregate's correction ran: {rows:?}"
    );
    // The other aggregate's deltas fold, and it goes live.
    let plan = fx.d.build_plan("agg2").await;
    fx.d.merge_all(&plan).await;
    let agg2: i64 =
        fx.d.ctl
            .query_one(
                "select id from transform_definitions where target_table like '%.agg2'",
                &[],
            )
            .await
            .expect("agg2")
            .get(0);
    assert!(
        build::try_complete(fx.d.pool(), agg2)
            .await
            .expect("complete")
    );
    assert_eq!(fx.status_of("agg2").await, "live");
    assert_eq!(fx.queue().await.len(), 1, "agg has not re-derived it");

    // Each later attempt fails the same way, after its backoff (skipped
    // here), until the fifth charged failure (the chunk queue's
    // `MAX_CHARGED_ATTEMPTS`) pauses the definition.
    for _ in 1..5 {
        fx.exec("update backfill_chunks set next_attempt_at = null where kind = 'correction'")
            .await;
        fx.worker_turns().await;
    }
    assert_eq!(fx.status_of("agg").await, "paused");
    assert_eq!(
        correcting("agg").await,
        Vec::<String>::new(),
        "a paused definition's row is the record of why, not a correction in progress"
    );
    fx.full_pass().await;
    assert_eq!(
        fx.queue().await.len(),
        1,
        "the paused definition's row keeps the correction queued"
    );
    let kept = fx.rows().await;
    assert!(
        kept.iter().any(|r| r.0 == "agg"
            && !r.1
            && r.3.as_deref().is_some_and(|e| e.contains("no_tombstones"))),
        "the row is the record of why: {kept:?}"
    );

    // The resume deletes the row and rebuilds the definition from the
    // source, which releases the correction: it leaves the queue, and the
    // definition ends `live` and right.
    fx.exec(&format!(
        "alter table {ledger} drop constraint no_tombstones"
    ))
    .await;
    trellis::staging::quarantine::resume_transform(fx.d.pool(), "agg")
        .await
        .expect("resume");
    for _ in 0..3 {
        fx.full_pass().await;
        fx.settle_builds().await;
    }
    assert_eq!(fx.status_of("agg").await, "live");
    assert!(
        fx.queue().await.is_empty(),
        "the resume released the correction"
    );
    assert!(fx.rows().await.is_empty());
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
    assert_eq!(correcting("agg").await, Vec::<String>::new());
    handle.shutdown().await.expect("shut down");
}
