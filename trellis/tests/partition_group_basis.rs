//! #1103 (G5): partition maintenance under a group-basis aggregate.
//!
//! A leaf or middle `DROP`, `DETACH`, `TRUNCATE` or `ATTACH`, a new leaf
//! written directly before its capture, a `DEFAULT` whose siblings changed,
//! and the complement sweep's correction are each corrected by recomputing
//! the groups that route to the node: the target's, those of undrained ring
//! images and those of the node's current rows, with a snapshot taken after
//! the operation as each group's basis (#1043 F4). Never by a zero.
//!
//! Every test starts from an empty source, built before
//! its tree is recorded, and seeds it through capture. The pass, the
//! drain and the engine are called by hand; nothing waits for convergence
//! (#297). The oracle is a from-scratch `GROUP BY` over the source, and the
//! hidden partials must never go negative.
//!
//! The cases are #1043 E1's variant-L scenarios (`P.*`), #958's lifecycle
//! cells (X12-7a two-level trees, X12-7b a `DEFAULT` operated on, X12-7c a
//! `HASH` re-split, X12-7e writes into the node while it is corrected), and
//! the sweep's interleavings from `tests/partition_sweep.rs`, with no
//! full-routing fallback (#1129).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::{Driver, Running};
use trellis::capture::partitions::{self, PendingCorrection};
use trellis::capture::reconcile::PassOutcome;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::interleave::PausePoint;
use trellis::staging::partition_correction::{self, Options, Outcome};
use trellis::{Trellis, TrellisOptions};

const SOURCE: &str = "public.items";
const DEFINITION: &str = "TRANSFORM agg FROM public.items GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n";

/// `items (id, g, v)` partitioned `kind` on `g`, empty.
fn head(kind: &str) -> String {
    format!(
        "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by {kind} (g);"
    )
}

/// Four leaves `items_p0`..`items_p3` for the groups 0-3, 4-7, 8-11 and
/// 12-15 (`HASH`: remainders 0-3 of modulus 4), empty.
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
    format!("{}{leaves}", head(kind))
}

/// 160 rows through the parent: ten per group 0-15, `v` the id.
const SEED: &str = "insert into public.items select i, i % 16, i from generate_series(1, 160) i";

fn int8() -> ValueType {
    ValueType::Integer(IntWidth::Int8)
}

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

struct Fx {
    d: Driver,
}

impl Fx {
    /// `schema` with the aggregate live on the group basis over it, `seed`
    /// written through capture and applied, and the tree recorded.
    async fn start(schema: &str, seed: &str) -> Fx {
        Self::start_with(schema, seed, &[DEFINITION]).await
    }

    async fn start_with(schema: &str, seed: &str, definitions: &[&str]) -> Fx {
        let d = Driver::start(
            schema,
            &[("id", int4()), ("g", int4()), ("v", int4())],
            definitions,
            &[SOURCE],
        )
        .await;
        let mut fx = Fx { d };
        let hidden: i64 = fx
            .d
            .ctl
            .query_one(
                "select count(*) from pg_tables where schemaname = $1 and tablename like 'agg\\_%'",
                &[&DEFAULT_SCHEMA],
            )
            .await
            .expect("hidden tables")
            .get(0);
        assert_eq!(hidden, 1, "one definition is on the group basis");
        // The first sight of the source records its tree and queues nothing.
        fx.pass().await;
        assert!(fx.queue().await.is_empty());
        if !seed.is_empty() {
            fx.exec(seed).await;
        }
        fx.d.settle().await;
        fx.assert_right("seeded").await;
        fx
    }

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

    async fn kinds(&self) -> Vec<String> {
        self.queue().await.into_iter().map(|c| c.kind).collect()
    }

    /// The groups whose target row differs from a `GROUP BY` over the source,
    /// as `group: have vs want`.
    async fn wrong_groups(&self) -> Vec<String> {
        self.d
            .rows(
                "with want as (select g, sum(v) as total, count(*) as n \
                               from public.items group by g), \
                      have as (select g, total, n from public.agg) \
                 select coalesce(w.g, h.g)::text || ': ' || coalesce(h.total::text, '-') \
                        || '/' || coalesce(h.n::text, '-') || ' vs ' \
                        || coalesce(w.total::text, '-') || '/' || coalesce(w.n::text, '-') \
                 from want w full join have h on h.g = w.g \
                 where w.total is distinct from h.total or w.n is distinct from h.n \
                 order by coalesce(w.g, h.g)",
            )
            .await
    }

    /// The hidden table, qualified.
    async fn hidden(&self) -> String {
        let id: i64 = self
            .d
            .ctl
            .query_one(
                "select id from transform_definitions where target_table = 'public.agg'",
                &[],
            )
            .await
            .expect("definition id")
            .get(0);
        format!("{DEFAULT_SCHEMA}.agg_{id}")
    }

    /// Hidden rows a delta drove below zero.
    async fn negative_partials(&self) -> Vec<String> {
        self.d
            .rows(&format!(
                "select g, __n, __count_1, __sum_1 from {} \
                 where __n < 0 or __count_1 < 0",
                self.hidden().await
            ))
            .await
    }

    /// The target equals the oracle, no partial is negative, and the
    /// definition is `live`.
    async fn assert_right(&self, what: &str) {
        assert_eq!(self.wrong_groups().await, Vec::<String>::new(), "{what}");
        assert_eq!(
            self.negative_partials().await,
            Vec::<String>::new(),
            "{what}"
        );
        assert_eq!(self.status().await, "live", "{what}");
    }

    async fn status(&self) -> String {
        self.d
            .ctl
            .query_one(
                "select status from transform_definitions where target_table = 'public.agg'",
                &[],
            )
            .await
            .expect("status")
            .get(0)
    }

    /// Runs the engine to its end, with no drain before it: a ring change
    /// staged before the operation is still undrained.
    async fn run(&self) -> Outcome {
        let outcome = partition_correction::run_pending(self.d.pool(), &Options::default())
            .await
            .expect("run the pending corrections");
        if outcome.remaining == 0 {
            assert_eq!(
                self.status().await,
                "live",
                "the correction's completion moves the definition back to live"
            );
        }
        let rows: i64 = self
            .d
            .ctl
            .query_one(
                "select count(*) from backfill_chunks bc \
                 join transform_definitions d on d.id = bc.definition_id \
                 where bc.kind = 'correction' and d.target_table = 'public.agg' and bc.done",
                &[],
            )
            .await
            .expect("correction rows")
            .get(0);
        assert_eq!(rows, 0, "a done correction row is deleted");
        outcome
    }

    /// The pass, a seal (which queues a truncated node's correction), the
    /// engine (before any drain) and a drain to quiescence.
    async fn correct(&mut self) -> Outcome {
        self.pass().await;
        let batch = self.d.seal().await;
        let outcome = self.run().await;
        self.drain(batch).await;
        outcome
    }

    /// Drains `batch`, sealed earlier and left undrained, then the rest of
    /// the ring to quiescence.
    async fn drain(&mut self, batch: i64) {
        self.d.drain(batch, "after").await;
        self.d.settle().await;
    }

    /// The groups the hidden table has, with whether each has a basis.
    async fn hidden_groups(&self) -> Vec<String> {
        self.d
            .rows(&format!(
                "select g, __n, __basis is not null from {} order by g",
                self.hidden().await
            ))
            .await
    }

    async fn exec(&self, sql: &str) {
        if sql.is_empty() {
            return;
        }
        self.d
            .ctl
            .batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    /// Two passes, each followed by the engine and a drain: the second finds
    /// what the first pass's corrections left (the sweep's convergence).
    async fn converge_twice(&mut self, what: &str) {
        assert!(
            !self.wrong_groups().await.is_empty(),
            "{what}: rows to correct"
        );
        self.correct().await;
        self.correct().await;
        self.assert_right(what).await;
        assert!(self.queue().await.is_empty(), "{what}");
    }
}

// --- Leaf and middle operations (#1043 E1 variant L, `P.*`) ---------------

/// `P.drop` / `P.detach`: a leaf dropped or detached with changes to it
/// still undrained in the ring. The recompute's basis sees their
/// transactions, so their pages are refused after it.
#[tokio::test]
async fn a_dropped_or_detached_leaf_with_undrained_changes_is_recomputed() {
    for kind in ["hash", "range", "list"] {
        for op in [
            "drop table public.items_p1",
            "alter table public.items detach partition public.items_p1",
        ] {
            let what = format!("{kind}: {op}");
            let mut fx = Fx::start(&tree(kind), SEED).await;
            // Undrained: an insert, an update and a delete in the leaf.
            fx.exec(
                "insert into public.items select 500 + i, g, 1 from \
                     (select distinct g from public.items_p1) s(g), generate_series(1, 2) i; \
                 update public.items_p1 set v = v + 100 where id % 3 = 0; \
                 delete from public.items_p1 where id % 5 = 0;",
            )
            .await;
            fx.exec(op).await;
            fx.pass().await;
            assert_eq!(fx.kinds().await, ["gone_node"], "{what}");
            let outcome = fx.run().await;
            assert_eq!(outcome.completed, 1, "{what}");
            assert!(fx.queue().await.is_empty(), "{what}");
            // The leaf's groups are recomputed to nothing before their
            // undrained changes drain, and stay so after.
            fx.d.settle().await;
            fx.assert_right(&what).await;
            if op.starts_with("alter") {
                // A write to the detached table stages nothing.
                fx.exec(
                    "insert into public.items_p1 select 900 + i, g, 1 from \
                         (select distinct g from public.items_p1) s(g), generate_series(1, 2) i",
                )
                .await;
                fx.d.settle().await;
                fx.assert_right(&what).await;
            }
        }
    }
}

/// `P.stale-new-group`: an undrained insert creates a group the target
/// doesn't have yet, in the leaf that is then dropped. The correction's set
/// takes the groups of undrained ring images, so the stale page is refused
/// rather than creating the group with a member the drop removed.
#[tokio::test]
async fn an_undrained_change_naming_a_new_group_of_a_dropped_leaf_is_refused() {
    let mut fx = Fx::start(
        &tree("range"),
        "insert into public.items select i, 4 + i % 2, i from generate_series(1, 20) i",
    )
    .await;
    // Group 6 exists only in the ring.
    fx.exec("insert into public.items values (100, 6, 4)").await;
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    fx.run().await;
    // The recompute wrote group 6 at zero with a basis, which refuses the
    // insert's page. Checked before the drain: once it has drained, the
    // zero-group collection removes the zero groups.
    assert_eq!(
        fx.hidden_groups().await,
        ["(4,0,t)", "(5,0,t)", "(6,0,t)"],
        "groups corrected: the target's and the ring's"
    );
    fx.d.settle().await;
    fx.assert_right("stale new group").await;
}

/// `P.truncate-leaf`: a leaf truncated with undrained changes, then a new
/// insert through the parent into the emptied leaf. The seal queues the
/// correction from the truncate's marker.
#[tokio::test]
async fn a_truncated_leaf_with_undrained_changes_is_recomputed() {
    for kind in ["hash", "range", "list"] {
        let mut fx = Fx::start(&tree(kind), SEED).await;
        fx.exec(
            "insert into public.items select 500 + i, g, 1 from \
                 (select distinct g from public.items_p2) s(g), generate_series(1, 2) i; \
             truncate public.items_p2;",
        )
        .await;
        let batch = fx.d.seal().await;
        assert_eq!(fx.kinds().await, ["truncated"], "{kind}");
        fx.exec(
            "insert into public.items select 700 + i, g, 3 from \
                 (select distinct g from public.items_p0) s(g), generate_series(1, 1) i",
        )
        .await;
        fx.run().await;
        fx.drain(batch).await;
        fx.assert_right(kind).await;
        assert!(fx.queue().await.is_empty(), "{kind}");
    }
}

/// `P.truncate-insert-txn`: `TRUNCATE` a leaf and `INSERT` into it in one
/// transaction. A zero of the leaf's groups would erase the new row, whose
/// change its basis then refuses; the recompute counts it.
#[tokio::test]
async fn a_truncate_and_an_insert_into_the_leaf_in_one_transaction_keep_the_new_row() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    fx.exec(
        "begin; truncate public.items_p0; \
         insert into public.items_p0 values (1000, 3, 99); \
         insert into public.items values (1001, 2, 98); commit;",
    )
    .await;
    let batch = fx.d.seal().await;
    assert_eq!(fx.kinds().await, ["truncated"]);
    fx.run().await;
    fx.drain(batch).await;
    fx.assert_right("truncate and insert").await;
    assert_eq!(
        fx.d.rows("select g, total, n from public.agg where g < 4 order by g")
            .await,
        ["(2,98,1)", "(3,99,1)"]
    );
}

/// A middle `TRUNCATE` corrects every group under the middle through its own
/// text, and leaves the other leaves' groups alone.
#[tokio::test]
async fn a_truncated_middle_is_recomputed_through_its_own_text() {
    let mut fx = Fx::start(
        &format!(
            "{} create table public.items_m partition of public.items for values from (0) to (8) \
                 partition by list (g); \
             create table public.items_m0 partition of public.items_m for values in (0, 1, 2, 3); \
             create table public.items_m1 partition of public.items_m for values in (4, 5, 6, 7); \
             create table public.items_p2 partition of public.items for values from (8) to (16);",
            head("range")
        ),
        SEED,
    )
    .await;
    fx.exec("truncate public.items_m").await;
    let batch = fx.d.seal().await;
    assert_eq!(fx.kinds().await, ["truncated"]);
    let outcome = fx.run().await;
    assert_eq!(outcome.keys, 8, "the middle's eight groups");
    fx.drain(batch).await;
    fx.assert_right("middle truncate").await;
}

/// `P.drop-recreate-txn`: a leaf dropped and re-created with rows in one
/// transaction; the new table's rows were written before any capture.
#[tokio::test]
async fn a_leaf_dropped_and_recreated_with_rows_in_one_transaction_is_recomputed() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    fx.exec(
        "begin; drop table public.items_p1; \
         create table public.items_p1 partition of public.items for values from (4) to (8); \
         insert into public.items_p1 select 2000 + i, 4 + i % 3, 5 from generate_series(1, 9) i; \
         commit;",
    )
    .await;
    fx.correct().await;
    fx.assert_right("drop and re-create").await;
}

/// `P.attach`: a loaded table attached as a leaf, with direct writes before
/// its capture, a write through the parent, and a direct write between the
/// pass that installs its capture and the correction.
#[tokio::test]
async fn an_attached_leaf_with_rows_is_recomputed() {
    for kind in ["hash", "range", "list"] {
        let mut fx = Fx::start(&tree(kind), SEED).await;
        fx.exec("alter table public.items detach partition public.items_p3")
            .await;
        fx.correct().await;
        fx.assert_right(kind).await;
        let bound: String =
            fx.d.ctl
                .query_one(
                    "select case $1 when 'hash' then 'for values with (modulus 4, remainder 3)' \
                                    when 'range' then 'for values from (12) to (16)' \
                                    else 'for values in (12, 13, 14, 15)' end",
                    &[&kind],
                )
                .await
                .expect("bound")
                .get(0);
        // Uncaptured writes to the detached table, then the attach.
        fx.exec(&format!(
            "insert into public.items_p3 select 3000 + i, g, 2 from \
                 (select distinct g from public.items_p3) s(g), generate_series(1, 3) i; \
             alter table public.items attach partition public.items_p3 {bound}; \
             insert into public.items select 4000 + i, g, 3 from \
                 (select distinct g from public.items_p3) s(g), generate_series(1, 2) i; \
             insert into public.items_p3 select 4500 + i, g, 4 from \
                 (select distinct g from public.items_p3) s(g), generate_series(1, 2) i;"
        ))
        .await;
        fx.pass().await;
        assert_eq!(fx.kinds().await, ["new_leaf"], "{kind}");
        // Captured now: a direct write between the pass and the correction.
        fx.exec(
            "insert into public.items_p3 select 4800 + i, g, 5 from \
                 (select distinct g from public.items_p3) s(g), generate_series(1, 1) i",
        )
        .await;
        fx.run().await;
        fx.d.settle().await;
        fx.assert_right(kind).await;
    }
}

/// A leaf created and written directly before its capture exists: only its
/// correction's read of the node's rows counts those rows.
#[tokio::test]
async fn a_new_leaf_with_direct_writes_before_its_capture_is_recomputed() {
    let mut fx = Fx::start(
        &format!(
            "{} create table public.items_p0 partition of public.items for values from (0) to (8);",
            head("range")
        ),
        "insert into public.items select i, i % 8, i from generate_series(1, 80) i",
    )
    .await;
    fx.exec(
        "create table public.items_p1 partition of public.items for values from (8) to (16); \
         insert into public.items_p1 select 5000 + i, 8 + i % 8, 1 from generate_series(1, 40) i;",
    )
    .await;
    assert!(
        !fx.wrong_groups().await.is_empty(),
        "the direct rows are missed"
    );
    fx.correct().await;
    fx.assert_right("new leaf").await;
}

/// `P.attach-late-enum`: a group first appears in the attached leaf after the
/// correction enumerated its groups, and drains before the recompute. The
/// group is outside the set, so its change applies as it is.
#[tokio::test]
async fn a_group_first_written_after_the_enumeration_is_kept() {
    let mut fx = Fx::start(
        &tree("range"),
        "insert into public.items select i, i % 14, i from generate_series(1, 140) i",
    )
    .await;
    fx.exec(
        "alter table public.items detach partition public.items_p3; \
         insert into public.items_p3 values (3000, 12, 1); \
         alter table public.items attach partition public.items_p3 for values from (12) to (16);",
    )
    .await;
    fx.pass().await;
    assert_eq!(fx.kinds().await, ["new_leaf"]);
    let mut run =
        fx.d.run_frozen(
            &[(PausePoint::BeforeCorrectionRecompute, "public.agg")],
            |pool| async move {
                Ok(
                    partition_correction::run_pending(&pool, &Options::default())
                        .await
                        .expect("run the corrections"),
                )
            },
        )
        .await;
    run.reached(PausePoint::BeforeCorrectionRecompute).await;
    // Group 15 is new, group 12 is in the set; both drain before the
    // recompute runs.
    fx.exec("insert into public.items values (3100, 15, 7), (3101, 12, 8)")
        .await;
    fx.d.settle().await;
    fx.d.release(&mut run, PausePoint::BeforeCorrectionRecompute)
        .await;
    assert_eq!(run.finish().await.completed, 1);
    fx.d.settle().await;
    fx.assert_right("late group").await;
}

/// A re-attach detected after a reader enumerated a queued `new_leaf`'s
/// groups, and before it recomputed them, queues its own entry (#1151): the
/// reader has started, so its enumeration misses a group the leaf gained
/// while it was detached, and the queued entry cannot absorb the operation.
#[tokio::test]
async fn a_reattach_detected_after_the_enumeration_queues_its_own_entry() {
    let mut fx = Fx::start(
        &tree("range"),
        "insert into public.items select i, i % 14, i from generate_series(1, 140) i",
    )
    .await;
    let reattach = "alter table public.items attach partition public.items_p3 \
                    for values from (12) to (16)";
    fx.exec(&format!(
        "alter table public.items detach partition public.items_p3; \
         insert into public.items_p3 values (3000, 12, 1); {reattach}"
    ))
    .await;
    fx.pass().await;
    assert_eq!(fx.kinds().await, ["new_leaf"]);
    let mut run =
        fx.d.run_frozen(
            &[(PausePoint::BeforeCorrectionRecompute, "public.agg")],
            |pool| async move {
                Ok(
                    partition_correction::run_pending(&pool, &Options::default())
                        .await
                        .expect("run the corrections"),
                )
            },
        )
        .await;
    run.reached(PausePoint::BeforeCorrectionRecompute).await;
    // Group 15 first appears in the leaf while it is detached: uncaptured,
    // and outside the set the paused reader enumerated.
    fx.exec(&format!(
        "alter table public.items detach partition public.items_p3; \
         insert into public.items_p3 values (3100, 15, 7); {reattach}"
    ))
    .await;
    fx.pass().await;
    assert_eq!(
        fx.kinds().await,
        ["new_leaf", "new_leaf"],
        "the started entry does not absorb the re-attach"
    );
    fx.d.release(&mut run, PausePoint::BeforeCorrectionRecompute)
        .await;
    run.finish().await;
    fx.correct().await;
    fx.assert_right("re-attach after the enumeration").await;
}

/// X12-7e: writes into the leaf land while its correction holds the
/// recomputed groups: one committed before the recompute's snapshot is
/// counted by it and refused by its basis; one after it applies.
#[tokio::test]
async fn writes_into_the_leaf_during_its_correction_converge() {
    let mut fx = Fx::start(&tree("hash"), SEED).await;
    let writer = fx.d.user().await;
    fx.exec("truncate public.items_p1").await;
    let batch = fx.d.seal().await;
    assert_eq!(fx.kinds().await, ["truncated"]);
    let groups: Vec<i32> =
        fx.d.ctl
            .query(
                "select g from generate_series(0, 15) g \
             where satisfies_hash_partition('public.items'::regclass, 4, 1, g) order by g",
                &[],
            )
            .await
            .expect("leaf groups")
            .iter()
            .map(|r| r.get(0))
            .collect();
    // Committed after the enumeration, before the recompute: counted.
    let mut run =
        fx.d.run_frozen(
            &[
                (PausePoint::BeforeCorrectionRecompute, "public.agg"),
                (PausePoint::BeforeCorrectionCommit, "public.agg"),
            ],
            |pool| async move {
                Ok(
                    partition_correction::run_pending(&pool, &Options::default())
                        .await
                        .expect("run the corrections"),
                )
            },
        )
        .await;
    run.reached(PausePoint::BeforeCorrectionRecompute).await;
    writer
        .execute(
            "insert into public.items values (6000, $1, 11)",
            &[&groups[0]],
        )
        .await
        .expect("write before the recompute");
    fx.d.release(&mut run, PausePoint::BeforeCorrectionRecompute)
        .await;
    run.reached(PausePoint::BeforeCorrectionCommit).await;
    assert_eq!(
        fx.status().await,
        "backfilling",
        "until the correction commits"
    );
    // While the recompute holds its groups: an insert into another of them
    // and an update of the row written above, both committed after its
    // snapshot.
    writer
        .batch_execute(&format!(
            "insert into public.items values (6001, {}, 12); \
             update public.items set v = v + 1 where id = 6000;",
            groups[1]
        ))
        .await
        .expect("write during the recompute");
    fx.d.release(&mut run, PausePoint::BeforeCorrectionCommit)
        .await;
    assert_eq!(run.finish().await.completed, 1);
    fx.drain(batch).await;
    fx.assert_right("writes during the correction").await;
}

/// How a leaf operation's correction meets the page in flight of changes the
/// operation removed (#1309).
#[derive(Debug, Clone, Copy)]
enum Race {
    /// The page holds the groups; the correction's recompute queues behind
    /// it, then reads after the page commits.
    CorrectionWaitsOnPage,
    /// The correction has recomputed and holds the groups; the page queues
    /// behind it, then reads the new bases.
    PageWaitsOnCorrection,
    /// The correction has enumerated its groups; the page then locks them,
    /// and the correction's recompute queues behind it.
    PageBetweenEnumerationAndRecompute,
}

/// #1309: a leaf `TRUNCATE`, `DETACH` or `DROP` while a page applying rows
/// of the leaf's groups, committed before the operation, is in flight. The
/// page is frozen right after its group lock or after its increments, and
/// meets the operation's correction each way round ([`Race`]); a write into
/// the truncated leaf after the operation, committed before the recompute,
/// drains after it. Two rows of an existing group and one of a group only
/// the ring names: whichever commits second, each row is counted once (the
/// #1117 soak's one failing run read as two rows counted twice).
#[tokio::test]
async fn a_leaf_operation_racing_a_page_in_flight_counts_each_row_once() {
    let mut wrong = Vec::new();
    for op in [
        "truncate public.items_p1",
        "alter table public.items detach partition public.items_p1",
        "drop table public.items_p1",
    ] {
        for page_point in [PausePoint::AfterEntryLock, PausePoint::AfterGroupUpsert] {
            for race in [
                Race::CorrectionWaitsOnPage,
                Race::PageWaitsOnCorrection,
                Race::PageBetweenEnumerationAndRecompute,
            ] {
                wrong.extend(leaf_operation_races_a_page(op, page_point, race).await);
            }
        }
    }
    assert_eq!(wrong, Vec::<String>::new());
}

/// Runs leaf operation `op` on `items_p1` and queues its correction. After a
/// truncate, whose correction the seal queues, a write into the emptied leaf
/// (to `old` and `new`) lands before the recompute. Returns the batch the
/// seal sealed, if any, undrained.
async fn operate(fx: &mut Fx, op: &str, old: i32, new: i32) -> Option<i64> {
    fx.exec(op).await;
    let truncate = op.starts_with("truncate");
    let mut sealed = None;
    if truncate {
        sealed = Some(fx.d.seal().await);
        fx.exec(&format!(
            "insert into public.items values (950, {old}, 19), (951, {new}, 23)"
        ))
        .await;
    } else {
        fx.pass().await;
    }
    assert_eq!(
        fx.kinds().await,
        [if truncate { "truncated" } else { "gone_node" }],
        "{op}: the correction is queued"
    );
    sealed
}

/// One cell of [`a_leaf_operation_racing_a_page_in_flight_counts_each_row_once`]:
/// what is wrong after it, if anything.
async fn leaf_operation_races_a_page(
    op: &str,
    page_point: PausePoint,
    race: Race,
) -> Option<String> {
    let what = format!("{op}, page at {page_point:?}, {race:?}");
    let mut fx = Fx::start(&tree("hash"), SEED).await;
    let leaf_groups: Vec<i32> =
        fx.d.ctl
            .query(
                "select g from generate_series(0, 63) g \
                 where satisfies_hash_partition('public.items'::regclass, 4, 1, g) order by g",
                &[],
            )
            .await
            .expect("leaf groups")
            .iter()
            .map(|r| r.get(0))
            .collect();
    let (old, new) = (
        leaf_groups[0],
        *leaf_groups.iter().find(|g| **g > 15).unwrap(),
    );
    // The page's changes: committed before the operation, sealed, undrained.
    fx.exec(&format!(
        "insert into public.items values (900, {old}, 13), (901, {old}, 17), (902, {new}, 5)"
    ))
    .await;
    let batch = fx.d.seal().await;
    let correct = |pool: trellis::Pool| async move {
        Ok(
            partition_correction::run_pending(&pool, &Options::default())
                .await
                .expect("run the corrections"),
        )
    };
    let page_points = [(page_point, "public.agg")];
    let marker;
    match race {
        Race::CorrectionWaitsOnPage => {
            let mut page = fx.d.drain_frozen(batch, "page", &page_points).await;
            let held = page.reached(page_point).await;
            marker = operate(&mut fx, op, old, new).await;
            let corrected = fx.d.run_frozen(&[], correct).await;
            fx.d.wait_blocked_behind(held.backend_pid).await;
            fx.d.release(&mut page, page_point).await;
            page.finish().await;
            assert_eq!(corrected.finish().await.completed, 1, "{what}");
        }
        Race::PageWaitsOnCorrection => {
            marker = operate(&mut fx, op, old, new).await;
            let point = PausePoint::BeforeCorrectionCommit;
            let mut corrected = fx.d.run_frozen(&[(point, "public.agg")], correct).await;
            let held = corrected.reached(point).await;
            let mut page = fx.d.drain_frozen(batch, "page", &page_points).await;
            fx.d.wait_blocked_behind(held.backend_pid).await;
            fx.d.release(&mut corrected, point).await;
            assert_eq!(corrected.finish().await.completed, 1, "{what}");
            page.reached(page_point).await;
            fx.d.release(&mut page, page_point).await;
            page.finish().await;
        }
        Race::PageBetweenEnumerationAndRecompute => {
            marker = operate(&mut fx, op, old, new).await;
            let point = PausePoint::BeforeCorrectionRecompute;
            let mut corrected = fx.d.run_frozen(&[(point, "public.agg")], correct).await;
            corrected.reached(point).await;
            let mut page = fx.d.drain_frozen(batch, "page", &page_points).await;
            let held = page.reached(page_point).await;
            fx.d.release(&mut corrected, point).await;
            fx.d.wait_blocked_behind(held.backend_pid).await;
            fx.d.release(&mut page, page_point).await;
            page.finish().await;
            assert_eq!(corrected.finish().await.completed, 1, "{what}");
        }
    }
    if let Some(marker) = marker {
        fx.d.drain(marker, "after").await;
    }
    fx.d.settle().await;
    assert!(fx.queue().await.is_empty(), "{what}");
    assert_eq!(fx.status().await, "live", "{what}");
    let (groups, partials) = (fx.wrong_groups().await, fx.negative_partials().await);
    (!groups.is_empty() || !partials.is_empty())
        .then(|| format!("{what}: {groups:?}, negative {partials:?}"))
}

/// #1309: a leaf truncate's correction whose recompute gives up on a group a
/// page holds (its `lock_timeout`) stays queued and runs again, so the
/// truncated rows are not left counted: under `run_pending` and under a
/// drain worker's claim alike. (The #1117 soak's model ran its
/// correction once, on a session with a 100 ms `lock_timeout`, and moved on
/// when it timed out.)
#[tokio::test]
async fn a_leaf_correction_that_times_out_behind_a_page_runs_again() {
    let mut fx = Fx::start(&tree("hash"), SEED).await;
    let old: i32 =
        fx.d.ctl
            .query_one(
                "select min(g) from generate_series(0, 15) g \
                 where satisfies_hash_partition('public.items'::regclass, 4, 1, g)",
                &[],
            )
            .await
            .expect("a leaf group")
            .get(0);
    fx.exec(&format!(
        "insert into public.items values (900, {old}, 13), (901, {old}, 17)"
    ))
    .await;
    let batch = fx.d.seal().await;
    let point = PausePoint::AfterEntryLock;
    let mut page =
        fx.d.drain_frozen(batch, "page", &[(point, "public.agg")])
            .await;
    page.reached(point).await;
    fx.exec("truncate public.items_p1").await;
    let marker = fx.d.seal().await;
    assert_eq!(fx.kinds().await, ["truncated"]);
    // A worker whose sessions wait 200 ms for a lock.
    let impatient = trellis::Pool::new(
        &Config::from_dsn(format!("{} options='-c lock_timeout=200'", fx.d.db.dsn()))
            .expect("valid dsn"),
    )
    .expect("a pool");
    let timed_out = partition_correction::run_pending(&impatient, &Options::default()).await;
    let err = timed_out.expect_err("the recompute waits on the page");
    assert!(format!("{err:?}").contains("lock timeout"), "{err:?}");
    assert_eq!(fx.kinds().await, ["truncated"], "still queued");
    // A drain worker's claim (`build::run_claimed`) meets the same timeout:
    // the failure is recorded as transient and backs off, uncharged, with
    // the correction still queued.
    let claimed = chunk_queue::claim_chunks_of(
        &**impatient.get().await.expect("pool"),
        "worker",
        1,
        &[chunk_queue::KIND_CORRECTION],
    )
    .await
    .expect("claim the correction");
    assert_eq!(claimed.len(), 1, "the correction's chunk");
    build::run_claimed(&impatient, &claimed[0], "worker", &REBUILD_OPTIONS).await;
    assert_eq!(
        fx.d.rows(
            "select done, claimed_by, charged, last_error like '%lock timeout%' \
             from backfill_chunks where kind = 'correction'"
        )
        .await,
        ["(f,,0,t)"],
        "backed off, uncharged"
    );
    assert_eq!(fx.kinds().await, ["truncated"], "still queued");
    // Its backoff has passed.
    fx.exec("update backfill_chunks set next_attempt_at = null")
        .await;
    fx.d.release(&mut page, point).await;
    page.finish().await;
    assert_eq!(fx.run().await.completed, 1);
    fx.drain(marker).await;
    fx.assert_right("the correction ran again").await;
    assert!(fx.queue().await.is_empty());
}

/// A leaf detached `CONCURRENTLY`: its correction waits while the detach is
/// pending, and runs once it is finalized.
#[tokio::test]
async fn a_detach_pending_leaf_waits_for_the_detach_to_finalize() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
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
    assert_eq!(fx.kinds().await, ["gone_node"]);
    let waiting = fx.run().await;
    assert_eq!((waiting.completed, waiting.pages), (0, 0));
    assert_eq!(
        fx.status().await,
        "backfilling",
        "from the scheduled correction"
    );
    holder.batch_execute("rollback").await.expect("release");
    fx.exec("alter table public.items detach partition public.items_p2 finalize")
        .await;
    fx.correct().await;
    fx.assert_right("detach finalized").await;
    assert!(fx.queue().await.is_empty());
}

// --- Two-level trees, DEFAULTs and HASH re-splits (#958) ------------------

/// `RANGE` then `HASH`: the root's ranges 0-7 and 8-15, the second a `HASH`
/// middle of four leaves.
fn two_level() -> String {
    format!(
        "{} create table public.items_lo partition of public.items for values from (0) to (8); \
         create table public.items_m partition of public.items for values from (8) to (16) \
             partition by hash (g); {}",
        head("range"),
        (0..4)
            .map(|r| format!(
                "create table public.items_m{r} partition of public.items_m \
                 for values with (modulus 4, remainder {r});"
            ))
            .collect::<String>()
    )
}

/// X12-7a: each lifecycle operation on a two-level tree, one per fixture.
#[tokio::test]
async fn every_operation_on_a_two_level_tree_converges() {
    for op in [
        "drop table public.items_m1",
        "alter table public.items_m detach partition public.items_m2",
        "truncate public.items_m3",
        "drop table public.items_m",
        "truncate public.items_m",
        "alter table public.items detach partition public.items_m; drop table public.items_m",
        "alter table public.items_m detach partition public.items_m0; \
         insert into public.items_m0 select 7000, g, 1 from generate_series(8, 15) g \
             where satisfies_hash_partition('public.items_m'::regclass, 4, 0, g) limit 1; \
         alter table public.items_m attach partition public.items_m0 \
             for values with (modulus 4, remainder 0)",
        "drop table public.items_m1; \
         create table public.items_m1 partition of public.items_m \
             for values with (modulus 4, remainder 1); \
         insert into public.items_m1 select 7100 + g, g, 2 from generate_series(8, 15) g \
             where satisfies_hash_partition('public.items_m'::regclass, 4, 1, g)",
    ] {
        let mut fx = Fx::start(&two_level(), SEED).await;
        fx.exec(op).await;
        fx.correct().await;
        fx.correct().await;
        fx.assert_right(op).await;
        assert!(fx.queue().await.is_empty(), "{op}");
    }
}

/// X12-7b: a `DEFAULT` as the node operated on: attached loaded, truncated,
/// detached, dropped; with siblings, and alone.
#[tokio::test]
async fn every_operation_on_a_default_converges() {
    let with_siblings = format!(
        "{} create table public.items_a partition of public.items for values from (0) to (8); \
         create table public.items_d partition of public.items default;",
        head("range")
    );
    let alone = format!(
        "{} create table public.items_d partition of public.items default;",
        head("list")
    );
    for (schema, op) in [
        (&with_siblings, "truncate public.items_d"),
        (&with_siblings, "drop table public.items_d"),
        (
            &with_siblings,
            "alter table public.items detach partition public.items_d",
        ),
        (
            &with_siblings,
            "alter table public.items detach partition public.items_d; \
             insert into public.items_d values (8000, 30, 1); \
             alter table public.items attach partition public.items_d default",
        ),
        (
            &with_siblings,
            "drop table public.items_a; insert into public.items values (8100, 2, 4)",
        ),
        (&alone, "truncate public.items_d"),
        (&alone, "drop table public.items_d"),
        (
            &alone,
            "alter table public.items detach partition public.items_d; \
             insert into public.items_d values (8000, 30, 1); \
             alter table public.items attach partition public.items_d default",
        ),
    ] {
        let mut fx = Fx::start(schema, SEED).await;
        fx.exec(op).await;
        fx.correct().await;
        fx.correct().await;
        fx.assert_right(op).await;
        assert!(fx.queue().await.is_empty(), "{op}");
    }
}

/// X12-7c: the `HASH` leaves of modulus 2 detached and re-attached, with new
/// tables, under modulus 4.
#[tokio::test]
async fn a_hash_resplit_converges() {
    let mut fx = Fx::start(
        &format!(
            "{} create table public.items_a partition of public.items \
                 for values with (modulus 2, remainder 0); \
             create table public.items_b partition of public.items \
                 for values with (modulus 2, remainder 1);",
            head("hash")
        ),
        SEED,
    )
    .await;
    fx.exec(
        "begin; \
         alter table public.items detach partition public.items_b; \
         create table public.items_b1 (like public.items including all); \
         create table public.items_b3 (like public.items including all); \
         insert into public.items_b1 select * from public.items_b \
             where satisfies_hash_partition('public.items'::regclass, 4, 1, g); \
         insert into public.items_b3 select * from public.items_b \
             where satisfies_hash_partition('public.items'::regclass, 4, 3, g) and id % 2 = 0; \
         alter table public.items attach partition public.items_b1 \
             for values with (modulus 4, remainder 1); \
         alter table public.items attach partition public.items_b3 \
             for values with (modulus 4, remainder 3); \
         drop table public.items_b; commit;",
    )
    .await;
    fx.correct().await;
    fx.correct().await;
    fx.assert_right("hash re-split").await;
    assert!(fx.queue().await.is_empty());
}

/// A new `HASH` leaf's correction routes by the leaf's text, which embeds its
/// middle's oid. A middle dropped before the correction runs makes that text
/// raise `XX000`; the correction is complete without work instead (#1148),
/// and the middle's own `gone_node`, queued by the next pass, recomputes the
/// region.
#[tokio::test]
async fn a_new_hash_leaf_whose_middle_was_dropped_completes_without_error() {
    let mut fx = Fx::start(&two_level(), SEED).await;
    fx.exec("drop table public.items_m1").await;
    fx.pass().await;
    fx.exec(
        "create table public.items_m1 partition of public.items_m \
         for values with (modulus 4, remainder 1)",
    )
    .await;
    fx.pass().await;
    fx.d.settle().await;
    assert_eq!(fx.kinds().await, ["gone_node", "new_leaf"]);
    fx.exec("drop table public.items_m").await;
    let outcome = fx.run().await;
    assert_eq!(outcome.completed, 2);
    assert!(fx.queue().await.is_empty());
    fx.correct().await;
    fx.assert_right("middle dropped under a queued new leaf")
        .await;
    assert!(fx.queue().await.is_empty());
}

// --- The complement sweep (tests/partition_sweep.rs on the group basis) ---

/// `items` partitioned `kind` on `g` with leaves for the groups 0-11 only,
/// empty; the rows (ten a group, none in the missing leaf); and the bound
/// of a leaf for the groups 12-15.
fn sweep_tree(kind: &str) -> (String, &'static str, &'static str) {
    let (leaves, rows, missing) = match kind {
        "hash" => (
            (0..3)
                .map(|r| {
                    format!(
                        "create table public.items_p{r} partition of public.items \
                         for values with (modulus 4, remainder {r});"
                    )
                })
                .collect::<String>(),
            "insert into public.items select i, i % 16, i from generate_series(1, 160) i \
             where not satisfies_hash_partition('public.items'::regclass, 4, 3, i % 16)",
            "for values with (modulus 4, remainder 3)",
        ),
        "range" => (
            (0..3)
                .map(|i| {
                    format!(
                        "create table public.items_p{i} partition of public.items \
                         for values from ({}) to ({});",
                        i * 4,
                        i * 4 + 4
                    )
                })
                .collect::<String>(),
            "insert into public.items select i, i % 12, i from generate_series(1, 120) i",
            "for values from (12) to (16)",
        ),
        _ => (
            (0..3)
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
            "insert into public.items select i, i % 12, i from generate_series(1, 120) i",
            "for values in (12, 13, 14, 15)",
        ),
    };
    (format!("{}{leaves}", head(kind)), rows, missing)
}

/// Rows through the parent for every group 0-15 the missing leaf takes.
const WRITE_MISSING: &str = "insert into public.items select 1000 + i, i % 16, 7 \
     from generate_series(1, 160) i where (i % 16) not in (select g from public.items)";

/// Blanks the recorded text of `name`, as a text fetch that raced the node's
/// detach leaves it.
async fn blank_text(fx: &Fx, name: &str) {
    fx.exec(&format!(
        "update partition_sources set nodes = jsonb_set(nodes, \
             array['nodes', (select (i - 1)::text from jsonb_array_elements(nodes->'nodes') \
                             with ordinality as e(n, i) \
                             where n->>'name' = '{name}'), 'constraint'], \
             'null'::jsonb)"
    ))
    .await;
}

#[tokio::test]
async fn a_leaf_created_written_and_dropped_between_two_passes_converges() {
    for kind in ["hash", "range", "list"] {
        let (schema, rows, missing) = sweep_tree(kind);
        let mut fx = Fx::start(&schema, rows).await;
        fx.exec(&format!(
            "create table public.items_new partition of public.items {missing}"
        ))
        .await;
        fx.exec(WRITE_MISSING).await;
        fx.d.settle().await;
        fx.exec("drop table public.items_new").await;
        assert!(!fx.wrong_groups().await.is_empty(), "{kind}");
        let outcome = fx.pass().await;
        assert!(outcome.partition_deep_checks.is_empty(), "{kind}");
        assert_eq!(fx.kinds().await, ["complement"], "{kind}");
        fx.run().await;
        fx.d.settle().await;
        fx.assert_right(kind).await;
        assert!(fx.queue().await.is_empty(), "{kind}");
        fx.pass().await;
        assert!(
            fx.queue().await.is_empty(),
            "{kind}: the next pass finds nothing"
        );
    }
}

/// A recorded node without a usable text, dropped: its groups route to no
/// leaf, or to the `DEFAULT` or new leaf that took its bound, and are
/// corrected through the complement or that node's own recompute.
#[tokio::test]
async fn a_gone_node_without_a_usable_text_converges() {
    for replacement in [
        "",
        "create table public.items_d partition of public.items default;",
        "create table public.items_again partition of public.items for values from (4) to (8);",
    ] {
        let (schema, rows, _) = sweep_tree("range");
        let mut fx = Fx::start(&schema, rows).await;
        blank_text(&fx, "public.items_p1").await;
        fx.exec("drop table public.items_p1").await;
        fx.exec(replacement).await;
        fx.pass().await;
        assert!(
            fx.queue()
                .await
                .iter()
                .any(|c| c.kind == "gone_node" && c.constraint.is_none()),
            "{replacement}"
        );
        fx.run().await;
        fx.d.settle().await;
        fx.correct().await;
        fx.assert_right(replacement).await;
        assert!(fx.queue().await.is_empty(), "{replacement}");
    }
}

#[tokio::test]
async fn a_hash_modulus_change_around_a_gone_node_without_a_usable_text_converges() {
    let mut fx = Fx::start(
        &format!(
            "{} create table public.items_a partition of public.items \
                 for values with (modulus 2, remainder 0); \
             create table public.items_b partition of public.items \
                 for values with (modulus 2, remainder 1);",
            head("hash")
        ),
        SEED,
    )
    .await;
    blank_text(&fx, "public.items_b").await;
    fx.exec(
        "drop table public.items_b; \
         create table public.items_b1 partition of public.items \
             for values with (modulus 4, remainder 1); \
         create table public.items_b3 partition of public.items \
             for values with (modulus 4, remainder 3); \
         insert into public.items select 5000 + i, i % 16, 3 from generate_series(1, 160) i \
             where not satisfies_hash_partition('public.items'::regclass, 2, 0, i % 16);",
    )
    .await;
    fx.d.settle().await;
    fx.exec("drop table public.items_b3").await;
    fx.converge_twice("hash modulus change").await;
}

#[tokio::test]
async fn a_middle_whose_children_are_partly_gone_converges() {
    let mut fx = Fx::start(
        &format!(
            "{} create table public.items_lo partition of public.items for values from (0) to (4); \
             create table public.items_m partition of public.items for values from (4) to (12) \
                 partition by list (g); \
             create table public.items_m1 partition of public.items_m for values in (4, 5, 6, 7); \
             create table public.items_m2 partition of public.items_m for values in (8, 9);",
            head("range")
        ),
        "insert into public.items select i, i % 10, i from generate_series(1, 100) i",
    )
    .await;
    blank_text(&fx, "public.items_m2").await;
    fx.exec(
        "drop table public.items_m2; \
         create table public.items_m3 partition of public.items_m for values in (10, 11); \
         insert into public.items select 5000 + i, 10 + i % 2, 1 from generate_series(1, 20) i;",
    )
    .await;
    fx.d.settle().await;
    fx.exec("drop table public.items_m3").await;
    fx.converge_twice("middle partly gone").await;
}

#[tokio::test]
async fn two_operations_in_one_pass_beside_a_default_converge() {
    let (schema, rows, _) = sweep_tree("range");
    let mut fx = Fx::start(
        &format!("{schema} create table public.items_d partition of public.items default;"),
        rows,
    )
    .await;
    blank_text(&fx, "public.items_p1").await;
    fx.exec(
        "drop table public.items_p1; \
         create table public.items_z partition of public.items for values from (4) to (6); \
         insert into public.items select 5000 + i, 4 + i % 4, 1 from generate_series(1, 40) i;",
    )
    .await;
    fx.d.settle().await;
    fx.exec("drop table public.items_z").await;
    fx.converge_twice("default beside two operations").await;
}

#[tokio::test]
async fn a_default_created_as_a_short_lived_leaf_is_dropped_converges() {
    let (schema, rows, missing) = sweep_tree("range");
    let mut fx = Fx::start(&schema, rows).await;
    fx.exec(&format!(
        "create table public.items_new partition of public.items {missing}"
    ))
    .await;
    fx.exec(WRITE_MISSING).await;
    fx.d.settle().await;
    fx.exec(
        "drop table public.items_new; \
         create table public.items_d partition of public.items default;",
    )
    .await;
    fx.converge_twice("default created with the leaf gone")
        .await;
}

#[tokio::test]
async fn a_default_created_written_and_dropped_between_two_passes_converges() {
    let (schema, rows, _) = sweep_tree("list");
    let mut fx = Fx::start(&schema, rows).await;
    fx.exec(
        "create table public.items_d partition of public.items default; \
         insert into public.items select 5000 + i, 20 + i % 5, 1 from generate_series(1, 50) i;",
    )
    .await;
    fx.d.settle().await;
    fx.exec("drop table public.items_d").await;
    fx.converge_twice("default created and dropped").await;
}

#[tokio::test]
async fn a_leaf_attached_written_and_dropped_between_two_passes_converges() {
    let (schema, rows, _) = sweep_tree("range");
    let mut fx = Fx::start(&schema, rows).await;
    fx.exec(
        "create table public.items_x (like public.items including all); \
         insert into public.items_x select 9000 + i, 12 + i % 4, 1 from generate_series(1, 8) i; \
         alter table public.items attach partition public.items_x for values from (12) to (16);",
    )
    .await;
    fx.exec("insert into public.items select 7000 + i, 12 + i % 4, 2 from generate_series(1, 8) i")
        .await;
    fx.d.settle().await;
    fx.exec(
        "alter table public.items detach partition public.items_x; \
         drop table public.items_x;",
    )
    .await;
    fx.converge_twice("attached and dropped").await;
}

#[tokio::test]
async fn a_complement_whose_hash_middle_is_dropped_completes_and_converges() {
    let mut fx = Fx::start(
        &format!(
            "{} create table public.items_lo partition of public.items for values from (0) to (4); \
             create table public.items_m partition of public.items for values from (4) to (20) \
                 partition by hash (g); \
             create table public.items_m0 partition of public.items_m \
                 for values with (modulus 4, remainder 0); \
             create table public.items_m1 partition of public.items_m \
                 for values with (modulus 4, remainder 1); \
             create table public.items_m2 partition of public.items_m \
                 for values with (modulus 4, remainder 2);",
            head("range")
        ),
        "insert into public.items select i, i % 20, i from generate_series(1, 200) i \
             where i % 20 < 4 \
                or not satisfies_hash_partition('public.items_m'::regclass, 4, 3, i % 20)",
    )
    .await;
    fx.exec(
        "create table public.items_m3 partition of public.items_m \
             for values with (modulus 4, remainder 3); \
         insert into public.items select 5000 + i, i % 20, 7 from generate_series(1, 200) i \
             where i % 20 >= 4 \
               and satisfies_hash_partition('public.items_m'::regclass, 4, 3, i % 20);",
    )
    .await;
    fx.d.settle().await;
    fx.exec("drop table public.items_m3").await;
    fx.d.settle().await;
    fx.pass().await;
    assert_eq!(fx.kinds().await, ["complement"]);
    fx.exec("drop table public.items_m").await;
    fx.run().await;
    assert!(
        fx.queue().await.is_empty(),
        "the complement completes once its HASH middle is gone"
    );
    assert!(!fx.wrong_groups().await.is_empty());
    fx.pass().await;
    assert_eq!(fx.kinds().await, ["gone_node"]);
    fx.run().await;
    fx.d.settle().await;
    fx.assert_right("complement under a dropped hash middle")
        .await;
    assert!(fx.queue().await.is_empty());
}

/// A recorded leaf whose capture was switched off by hand is a disruption
/// (#707, #1200): the reader pauses, nothing is reinstalled or rebuilt, and
/// the install's new-nodes shortcut is for nodes the recorded tree never
/// held.
#[tokio::test]
async fn a_recorded_leaf_whose_capture_was_switched_off_pauses_the_reader() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    fx.exec("alter table public.items_p1 disable trigger all")
        .await;
    fx.pass().await;
    assert!(fx.queue().await.is_empty(), "no lifecycle operation");
    assert_eq!(fx.status().await, "paused");
    let jobs =
        fx.d.rows(
            "select kind || ': ' || coalesce(last_error, '') from backfill_chunks \
             where definition_id = (select id from transform_definitions \
                                    where target_table = 'public.agg')",
        )
        .await;
    assert!(jobs.is_empty(), "no rebuild starts: {jobs:?}");
    let disabled =
        fx.d.rows(
            "select count(*)::text from pg_trigger \
             where tgrelid = 'public.items_p1'::regclass and tgenabled <> 'A' \
               and tgname like 'trellis_capture_%'",
        )
        .await;
    assert_eq!(disabled, ["(5)"], "nothing reinstalls");
}

/// #1147: a new leaf, empty or loaded before its capture, is corrected in the
/// pass that sees it, leaf alone. The reinstall of its capture records no
/// capture gate and rebuilds no reader (either would re-read every key of the
/// source for each reader): the aggregate stays `live` through the pass, and
/// the oracle holds after the correction.
#[tokio::test]
async fn a_new_leaf_costs_no_rebuild_and_no_source_wide_enumeration() {
    for loaded in [false, true] {
        let mut fx = Fx::start(&tree("list"), SEED).await;
        fx.exec("create table public.items_p4 partition of public.items for values in (16, 17)")
            .await;
        if loaded {
            fx.exec("insert into public.items_p4 select 9000 + i, 16 + i % 2, 3 + i from generate_series(1, 40) i")
                .await;
        }
        // The harness never lets the install's own gate go; what counts is
        // whether the pass records another.
        let parked = "select gate_lsn::text from capture_gates order by table_name";
        let before = fx.d.rows(parked).await;
        fx.pass().await;
        assert_eq!(fx.kinds().await, ["new_leaf"], "loaded: {loaded}");
        let state =
            fx.d.rows(
                "select status || '/' || coalesce(build, '-') from transform_definitions \
                 order by id",
            )
            .await;
        assert_eq!(state, ["(live/-)"], "loaded: {loaded}");
        let after = fx.d.rows(parked).await;
        assert!(
            after.iter().all(|gate| before.contains(gate)),
            "no gate recorded, so no whole-source re-read: loaded {loaded}, \
             before {before:?}, after {after:?}"
        );
        assert_eq!(
            fx.d.rows("select kind from backfill_chunks where kind <> 'correction'")
                .await,
            Vec::<String>::new(),
            "no rebuild: loaded {loaded}"
        );
        fx.run().await;
        fx.d.settle().await;
        trellis::staging::build::settle_builds(fx.d.pool()).await;
        fx.d.settle().await;
        fx.assert_right("group basis").await;
        assert_eq!(
            fx.d.rows("select count(*) from public.agg where g in (16, 17)")
                .await,
            [if loaded { "(2)" } else { "(0)" }]
        );
    }
}

/// `--ignored`: what a leaf's correction costs on the group basis (#1043
/// E2's world): a `RANGE` leaf of `TRELLIS_MEASURE_LEAF_ROWS` rows (1M by
/// default) over 1,560 groups beside a leaf of a tenth as many over 1,000
/// groups, the group key the partition key with an index leading on it (the
/// primary key). The leaf is loaded outside capture and attached, then
/// detached, attached again, truncated, and a second loaded leaf attached and
/// dropped; each correction is timed with the WAL it wrote, and the target is
/// checked against the oracle after each. Prints `MEASURE` lines.
#[tokio::test]
#[ignore = "a measurement: run with logbook reserve and a memory cap"]
async fn measure_the_correction_of_a_large_leaf() {
    let rows: i64 = std::env::var("TRELLIS_MEASURE_LEAF_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let mut fx = Fx::start(
        &format!(
            "{} create table public.items_other partition of public.items \
                 for values from (1560) to (50000);",
            head("range")
        ),
        &format!(
            "insert into public.items select i, 1560 + (i * 7) % 1000, i % 97 \
             from generate_series(1, {}) i",
            rows / 10
        ),
    )
    .await;
    async fn lsn(fx: &Fx) -> i64 {
        fx.d.ctl
            .query_one("select (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint", &[])
            .await
            .expect("lsn")
            .get(0)
    }
    async fn load(fx: &Fx, table: &str, rows: i64) {
        fx.exec(&format!(
            "create table public.{table} (like public.items including all, \
                 check (g >= 0 and g < 1560)); \
             insert into public.{table} select 10000000 + i, i % 1560, i % 89 \
                 from generate_series(1, {rows}) i; \
             analyze public.{table};"
        ))
        .await;
    }
    async fn timed(fx: &mut Fx, op: &str, rows: i64, sealed: Option<i64>) {
        let before = lsn(fx).await;
        let started = Instant::now();
        let outcome = fx.run().await;
        let took = started.elapsed();
        let wal = lsn(fx).await - before;
        eprintln!(
            "MEASURE op={op} leaf_rows={rows} groups={} corrections={} engine_ms={:.1} \
             engine_wal_mb={:.2}",
            outcome.keys,
            outcome.completed,
            took.as_secs_f64() * 1e3,
            wal as f64 / 1e6,
        );
        match sealed {
            Some(batch) => fx.drain(batch).await,
            None => fx.d.settle().await,
        }
        fx.assert_right(op).await;
    }
    load(&fx, "items_big", rows).await;
    fx.exec(
        "alter table public.items attach partition public.items_big for values from (0) to (1560)",
    )
    .await;
    fx.pass().await;
    timed(&mut fx, "attach", rows, None).await;

    fx.exec("alter table public.items detach partition public.items_big")
        .await;
    fx.pass().await;
    timed(&mut fx, "detach", rows, None).await;

    fx.exec(
        "alter table public.items attach partition public.items_big for values from (0) to (1560)",
    )
    .await;
    fx.pass().await;
    timed(&mut fx, "reattach", rows, None).await;

    fx.exec("truncate public.items_big").await;
    let batch = fx.d.seal().await;
    timed(&mut fx, "truncate", rows, Some(batch)).await;

    fx.exec("drop table public.items_big").await;
    fx.pass().await;
    fx.run().await;
    fx.d.settle().await;
    load(&fx, "items_big2", rows).await;
    fx.exec(
        "alter table public.items attach partition public.items_big2 for values from (0) to (1560)",
    )
    .await;
    fx.pass().await;
    fx.run().await;
    fx.d.settle().await;
    fx.assert_right("second attach").await;
    fx.exec("drop table public.items_big2").await;
    fx.pass().await;
    timed(&mut fx, "drop", rows, None).await;
}

/// A node in the tree is read directly only while it is there as recorded:
/// a leaf attached (its correction queued) and detached again before the
/// correction runs is read through the root, so the detached table's rows,
/// one written after the detach, aren't counted.
#[tokio::test]
async fn a_leaf_detached_before_its_correction_runs_is_read_through_the_root() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    fx.exec("alter table public.items detach partition public.items_p3")
        .await;
    fx.correct().await;
    fx.exec(
        "alter table public.items attach partition public.items_p3 for values from (12) to (16)",
    )
    .await;
    fx.pass().await;
    assert_eq!(fx.kinds().await, ["new_leaf"]);
    fx.exec(
        "alter table public.items detach partition public.items_p3; \
         insert into public.items_p3 values (9000, 12, 5);",
    )
    .await;
    fx.run().await;
    fx.d.settle().await;
    fx.assert_right("detached before the correction").await;
    fx.correct().await;
    fx.assert_right("after the detach's own correction").await;
    assert!(fx.queue().await.is_empty());
}

/// A node is read directly only while the whole tree is as recorded, not
/// just its own `pg_inherits` row: a `DEFAULT`'s truncate correction
/// enumerates its groups under its old text, a sibling is attached with rows
/// for two of them, and the sibling's correction commits first. Read alone,
/// the `DEFAULT` would zero those two groups after the sibling's correction
/// counted them, with nothing queued to repair it.
#[tokio::test]
async fn a_default_narrowed_before_its_recompute_is_read_through_the_root() {
    let mut fx = Fx::start(
        &format!(
            "{} create table public.items_a partition of public.items for values from (0) to (8); \
             create table public.items_d partition of public.items default;",
            head("range")
        ),
        SEED,
    )
    .await;
    fx.exec("truncate public.items_d").await;
    let batch = fx.d.seal().await;
    assert_eq!(fx.kinds().await, ["truncated"]);
    fx.drain(batch).await;
    let mut run =
        fx.d.run_frozen(
            &[(PausePoint::BeforeCorrectionRecompute, "public.agg")],
            |pool| async move {
                Ok(
                    partition_correction::run_pending(&pool, &Options::default())
                        .await
                        .expect("run the corrections"),
                )
            },
        )
        .await;
    run.reached(PausePoint::BeforeCorrectionRecompute).await;
    fx.exec(
        "create table public.items_s (like public.items including all); \
         insert into public.items_s values (9100, 8, 5), (9101, 9, 6); \
         alter table public.items attach partition public.items_s for values from (8) to (10);",
    )
    .await;
    fx.pass().await;
    assert_eq!(fx.kinds().await, ["truncated", "new_leaf", "new_leaf"]);
    // The sibling's and the narrowed `DEFAULT`'s corrections; the frozen
    // one's row is claimed.
    let others = partition_correction::run_pending(fx.d.pool(), &Options::default())
        .await
        .expect("the sibling's correction");
    assert_eq!((others.completed, others.remaining), (2, 1));
    fx.d.settle().await;
    fx.d.release(&mut run, PausePoint::BeforeCorrectionRecompute)
        .await;
    assert_eq!(run.finish().await.completed, 1);
    fx.d.settle().await;
    fx.assert_right("a DEFAULT narrowed before its recompute")
        .await;
    assert!(fx.queue().await.is_empty());
}

// --- A rebuild of a partitioned source: one transaction per leaf (#1101) ---

const REBUILD_OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 1_000,
    drain_batch_cap: usize::MAX,
    heartbeat_interval: Duration::from_secs(5),
    reclaim_ttl: Duration::from_secs(60),
};

impl Fx {
    /// Damages every group: its partials and its visible row, each by a
    /// different amount.
    async fn damage_every_group(&self) {
        self.exec(&format!(
            "update {} set __sum_1 = __sum_1 + 7, __count_1 = __count_1 + 1; \
             update public.agg set total = total + 7, n = n + 1",
            self.hidden().await
        ))
        .await;
        assert_eq!(self.wrong_groups().await.len(), 16, "every group is wrong");
    }

    /// `Trellis::request_backfill` of the source, the rebuild a user asks
    /// for: one sweep job, and the definition `backfilling` from the call.
    async fn request_rebuild(&self) {
        let trellis = Trellis::connect(
            Config::from_dsn(self.d.db.dsn().to_string()).expect("valid dsn"),
            TrellisOptions::default(),
        )
        .await
        .expect("connect a Trellis");
        trellis
            .request_backfill("items")
            .await
            .expect("request a rebuild");
        assert_eq!(self.status().await, "backfilling", "from the call");
        assert_eq!(
            self.d
                .rows("select kind from backfill_chunks where not done")
                .await,
            ["(sweep)"],
            "a group-basis rebuild is one job, with no plan"
        );
    }

    /// Claims the rebuild's job and runs it in its own task, frozen at each
    /// of `points` on the target once reached.
    async fn rebuild_frozen(&self, points: &[PausePoint]) -> Running<()> {
        let points: Vec<(PausePoint, &str)> = points.iter().map(|p| (*p, "public.agg")).collect();
        self.d
            .run_frozen(&points, |pool| async move {
                let claimed = chunk_queue::claim_chunks_of(
                    &**pool.get().await.expect("pool"),
                    "rebuild",
                    1,
                    &[chunk_queue::KIND_SWEEP],
                )
                .await
                .expect("claim the rebuild");
                assert_eq!(claimed.len(), 1, "one rebuild job to claim");
                build::run_claimed(&pool, &claimed[0], "rebuild", &REBUILD_OPTIONS).await;
                Ok(())
            })
            .await
    }

    /// Runs the rebuild to its end.
    async fn rebuild(&self) {
        self.rebuild_frozen(&[]).await.finish().await;
    }

    /// The definition is `live`, the job is gone and the target is right.
    async fn assert_rebuilt(&mut self, what: &str) {
        self.d.settle().await;
        let left = self
            .d
            .rows("select kind, done, lo, last_error from backfill_chunks")
            .await;
        assert_eq!(
            self.d.rows("select count(*) from backfill_chunks").await,
            ["(0)"],
            "{what}: the job is done and deleted: {left:?}"
        );
        self.assert_right(what).await;
    }

    /// The groups of `wrong_groups` as numbers.
    async fn wrong_numbers(&self) -> Vec<i32> {
        self.wrong_groups()
            .await
            .iter()
            .map(|line| {
                line.trim_start_matches(['(', '"'])
                    .split(':')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap_or_else(|_| panic!("a group number in {line}"))
            })
            .collect()
    }
}

/// A rebuild of a partitioned source repairs every leaf's damaged groups, on
/// every kind of partitioning, and ends `live` with no job behind.
#[tokio::test]
async fn a_rebuild_repairs_every_leaf_of_a_partitioned_source() {
    for kind in ["list", "range", "hash"] {
        let mut fx = Fx::start(&tree(kind), SEED).await;
        fx.damage_every_group().await;
        fx.request_rebuild().await;
        fx.rebuild().await;
        fx.assert_rebuilt(kind).await;
    }
}

/// A two-level tree, and a tree with a `DEFAULT` leaf.
#[tokio::test]
async fn a_rebuild_repairs_a_two_level_tree_and_a_default_leaf() {
    let default = format!(
        "{} create table public.items_a partition of public.items for values from (0) to (8); \
         create table public.items_d partition of public.items default;",
        head("range")
    );
    for (what, ddl) in [("two-level", two_level()), ("default", default)] {
        let mut fx = Fx::start(&ddl, SEED).await;
        fx.damage_every_group().await;
        fx.request_rebuild().await;
        fx.rebuild().await;
        fx.assert_rebuilt(what).await;
    }
}

/// The decision's test: a page between two leaves' transactions. The first
/// leaf's groups are new and the rest old; a change of the first leaf's
/// committed before the rebuild is refused by the new basis (counted once),
/// one committed after applies as a delta; the second leaf's rebuild then
/// counts a page that applied to its old basis.
#[tokio::test]
async fn a_page_between_two_leaves_transactions_is_exact_per_group() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    fx.damage_every_group().await;
    // Committed before the rebuild's snapshots, still undrained: group 1 is
    // the first leaf's, group 5 the second's.
    fx.exec("insert into public.items values (1001, 1, 100), (1005, 5, 100)")
        .await;
    fx.request_rebuild().await;
    let mut rebuilding = fx
        .rebuild_frozen(&[PausePoint::BetweenRebuildStructures])
        .await;
    rebuilding
        .reached(PausePoint::BetweenRebuildStructures)
        .await;
    assert_eq!(fx.status().await, "backfilling");
    let wrong = fx.wrong_numbers().await;
    assert_eq!(
        wrong,
        (4..16).collect::<Vec<_>>(),
        "the first leaf's groups are new (and count the undrained write), the others old"
    );
    // Both undrained changes drain now: group 1's is refused by the new
    // basis, group 5's applies as a delta to the old partials.
    // A change committed after the first leaf's snapshot applies as a delta.
    fx.exec("insert into public.items values (1002, 2, 7), (1006, 6, 7)")
        .await;
    let batch = fx.d.seal().await;
    fx.d.drain(batch, "between").await;
    assert_eq!(
        fx.wrong_numbers().await,
        (4..16).collect::<Vec<_>>(),
        "group 1 counted once and group 2 by its delta; the old groups still wrong"
    );
    fx.d.release(&mut rebuilding, PausePoint::BetweenRebuildStructures)
        .await;
    rebuilding.finish().await;
    fx.assert_rebuilt("after the last leaf").await;
}

/// A group that routes to no leaf: a visible row nothing wrote, and a hidden
/// row with partials the source doesn't have. The remainder removes the
/// visible row and zeroes the hidden one (R4).
#[tokio::test]
async fn a_group_that_routes_to_no_leaf_is_zeroed_by_the_remainder() {
    for kind in ["list", "range"] {
        let mut fx = Fx::start(&tree(kind), SEED).await;
        fx.exec(&format!(
            "insert into public.agg (g, total, n) values (99, 5, 1); \
             insert into {} (g, __n, __count_1, __sum_1) values (98, 3, 3, 30)",
            fx.hidden().await
        ))
        .await;
        assert_eq!(
            fx.d.rows("select g from public.agg where g >= 98").await,
            ["(99)"],
            "{kind}: the visible row is there before"
        );
        fx.request_rebuild().await;
        fx.rebuild().await;
        fx.assert_rebuilt(kind).await;
        assert_eq!(
            fx.d.rows("select g from public.agg where g >= 98").await,
            Vec::<String>::new(),
            "{kind}: the visible row is removed"
        );
        assert_eq!(
            fx.d.rows(&format!(
                "select g, __n, __count_1, __sum_1 from {} \
                 where g >= 98 and (__n, __count_1, __sum_1) <> (0, 0, 0)",
                fx.hidden().await
            ))
            .await,
            Vec::<String>::new(),
            "{kind}: no hidden row is left with partials"
        );
    }
}

/// Partition DDL between two leaves' transactions: a detach with a row
/// written to the detached table, a drop and an attach. The rebuild and the
/// corrections converge and neither deadlocks the other.
#[tokio::test]
async fn partition_ddl_between_two_leaves_transactions_converges() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    fx.damage_every_group().await;
    fx.request_rebuild().await;
    let mut rebuilding = fx
        .rebuild_frozen(&[PausePoint::BetweenRebuildStructures])
        .await;
    rebuilding
        .reached(PausePoint::BetweenRebuildStructures)
        .await;
    fx.exec(
        "alter table public.items detach partition public.items_p2; \
         insert into public.items_p2 values (9000, 8, 5); \
         drop table public.items_p3; \
         alter table public.items attach partition public.items_p2 \
             for values from (8) to (12)",
    )
    .await;
    fx.pass().await;
    fx.d.release(&mut rebuilding, PausePoint::BetweenRebuildStructures)
        .await;
    rebuilding.finish().await;
    fx.correct().await;
    fx.correct().await;
    fx.d.settle().await;
    assert_eq!(fx.d.deadlocks_logged(), Vec::<String>::new());
    assert!(fx.queue().await.is_empty());
    assert!(
        fx.d.rows("select count(*) from backfill_chunks where not done")
            .await
            == ["(0)"]
    );
    fx.assert_right("after the DDL and the rebuild").await;
}

/// A job killed after leaf k committed resumes after it: the cursor holds
/// the last leaf done, and the leaves before it are not written again (their
/// groups keep their bases).
#[tokio::test]
async fn a_killed_rebuild_resumes_after_the_last_committed_leaf() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    fx.damage_every_group().await;
    fx.request_rebuild().await;
    let mut rebuilding = fx
        .rebuild_frozen(&[PausePoint::BetweenRebuildStructures])
        .await;
    rebuilding
        .reached(PausePoint::BetweenRebuildStructures)
        .await;
    let hidden = fx.hidden().await;
    let bases = format!("select g, __basis::text from {hidden} where g < 4 order by g");
    let first_leaf = fx.d.rows(&bases).await;
    assert!(first_leaf.iter().all(|row| !row.ends_with(",)")));
    let cursor =
        fx.d.rows("select lo from backfill_chunks where not done")
            .await;
    assert_ne!(cursor, ["()"], "the cursor holds the last leaf done");
    rebuilding.kill().await;
    fx.exec("update backfill_chunks set claimed_by = null, claimed_at = null")
        .await;
    assert_eq!(fx.status().await, "backfilling");
    fx.d.settle().await;
    // The first leaf's groups are right and the others are not, still.
    assert_eq!(fx.wrong_numbers().await, (4..16).collect::<Vec<_>>());
    fx.rebuild().await;
    fx.assert_rebuilt("after the resumed job").await;
    assert_eq!(
        fx.d.rows(&bases).await,
        first_leaf,
        "the first leaf was not written again"
    );
}

/// A leaf with no usable constraint text can't have its groups routed. The
/// job halts rather than rebuilding the source in one statement.
#[tokio::test]
async fn a_leaf_without_a_constraint_text_halts_the_rebuild() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    fx.damage_every_group().await;
    blank_text(&fx, "public.items_p1").await;
    fx.request_rebuild().await;
    fx.rebuild().await;
    fx.d.settle().await;
    let error =
        fx.d.rows("select coalesce(string_agg(last_error, '; '), '') from backfill_chunks")
            .await;
    assert!(
        error.concat().contains("constraint text"),
        "the job names the leaf's missing text: {error:?}"
    );
    assert_eq!(
        fx.wrong_groups().await.len(),
        16,
        "nothing was written: no leaf was rebuilt"
    );
}

/// Changes still undrained when the rebuild runs, in three leaves: an update
/// of the first leaf's groups 0 and 1, an update of group 9, and the delete
/// of every row of group 5, whose hidden and visible rows were removed by
/// hand, so that only the ring's old images name it. Each leaf's transaction
/// takes the image groups that route to it and no others: a later leaf's
/// `GROUP BY` must not zero the first leaf's groups, and group 5 is written
/// at zero with a basis, which refuses the deletes' pages.
#[tokio::test]
async fn undrained_changes_in_several_leaves_are_each_counted_once() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    let hidden = fx.hidden().await;
    fx.exec(&format!(
        "delete from {hidden} where g = 5; delete from public.agg where g = 5"
    ))
    .await;
    fx.exec(
        "update public.items set v = v + 1 where g in (0, 1); \
         update public.items set v = v + 2 where g = 9; \
         delete from public.items where g = 5;",
    )
    .await;
    fx.request_rebuild().await;
    fx.rebuild().await;
    assert_eq!(
        fx.wrong_groups().await,
        Vec::<String>::new(),
        "right before the pages drain"
    );
    fx.assert_rebuilt("after the pages drain").await;
}

/// The groups of a leaf that only the target or the leaf names: a visible
/// row of a group the source never had (6), a hidden row with partials of
/// another (7), and a group whose hidden and visible rows are gone while its
/// rows are in the leaf (3). No ring image names any of them. Each leaf's
/// transaction takes them from its routed hidden and visible rows and from
/// its own rows.
#[tokio::test]
async fn a_rebuild_repairs_the_groups_only_the_target_or_the_leaf_names() {
    for kind in ["list", "range", "hash"] {
        let mut fx = Fx::start(
            &tree(kind),
            "insert into public.items select i, i % 16, i from generate_series(1, 160) i \
             where i % 16 not in (6, 7)",
        )
        .await;
        let hidden = fx.hidden().await;
        fx.exec(&format!(
            "delete from {hidden} where g in (3, 6, 7); delete from public.agg where g = 3; \
             insert into public.agg (g, total, n) values (6, 5, 1); \
             insert into {hidden} (g, __n, __count_1, __sum_1) values (7, 3, 3, 30)"
        ))
        .await;
        assert_eq!(fx.wrong_numbers().await, [3, 6], "{kind}");
        fx.request_rebuild().await;
        fx.rebuild().await;
        fx.assert_rebuilt(kind).await;
        assert_eq!(
            fx.d.rows(&format!(
                "select g from {hidden} where g in (6, 7) \
                 and (__n, __count_1, __sum_1) <> (0, 0, 0)"
            ))
            .await,
            Vec::<String>::new(),
            "{kind}: no hidden row without source rows keeps partials"
        );
    }
}

/// A leaf detached with its rows between two leaves' transactions, its
/// correction run before the rebuild reaches it. The leaf no longer is as
/// recorded, so its turn reads its groups through the root and writes them
/// at zero, as the correction did: read directly, the detached table's rows
/// would be counted again, with nothing left to repair them.
#[tokio::test]
async fn a_leaf_detached_and_corrected_before_its_turn_is_read_through_the_root() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    fx.damage_every_group().await;
    fx.request_rebuild().await;
    let mut rebuilding = fx
        .rebuild_frozen(&[PausePoint::BetweenRebuildStructures])
        .await;
    rebuilding
        .reached(PausePoint::BetweenRebuildStructures)
        .await;
    fx.exec("alter table public.items detach partition public.items_p2")
        .await;
    fx.pass().await;
    assert_eq!(fx.kinds().await, ["gone_node"]);
    let outcome = partition_correction::run_pending(fx.d.pool(), &Options::default())
        .await
        .expect("run the detach's correction");
    assert_eq!(outcome.completed, 1, "corrected before the rebuild's turn");
    fx.d.settle().await;
    fx.d.release(&mut rebuilding, PausePoint::BetweenRebuildStructures)
        .await;
    rebuilding.finish().await;
    fx.assert_rebuilt("the detached leaf's turn").await;
    assert!(fx.queue().await.is_empty());
}

/// Groups with a `NULL` part read through the root, one probe each: a
/// visible row of the remainder, and every group of the leaves after a
/// detach and re-attach between two leaves' transactions (the tree is no
/// longer as recorded). Each probe matches the source's own `NULL`s.
#[tokio::test]
async fn groups_with_a_null_part_read_through_the_root_are_exact() {
    let schema = format!(
        "create table public.items (id integer not null, g integer not null, h integer, \
         v integer not null, primary key (g, id)) partition by range (g); \
         create index on public.items (g, h); {}",
        (0..4)
            .map(|i| format!(
                "create table public.items_p{i} partition of public.items \
                 for values from ({}) to ({});",
                i * 4,
                i * 4 + 4
            ))
            .collect::<String>()
    );
    let d = Driver::start(
        &schema,
        &[("id", int4()), ("g", int4()), ("h", int4()), ("v", int4())],
        &["TRANSFORM agg FROM public.items GROUP BY g, h WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n"],
        &[SOURCE],
    )
    .await;
    let mut fx = Fx { d };
    fx.pass().await;
    fx.exec(
        "insert into public.items select i, i % 16, nullif(i % 3, 0), i \
         from generate_series(1, 160) i",
    )
    .await;
    fx.d.settle().await;
    let wrong = "with want as (select g, h, sum(v) as total, count(*) as n \
                               from public.items group by g, h) \
                 select coalesce(w.g, t.g)::text || '/' || coalesce(w.h, t.h)::text \
                 from want w full join public.agg t \
                   on t.g = w.g and t.h is not distinct from w.h \
                 where w.total is distinct from t.total or w.n is distinct from t.n \
                 order by 1";
    assert_eq!(fx.d.rows(wrong).await, Vec::<String>::new(), "seeded");
    let hidden = fx.hidden().await;
    fx.exec(&format!(
        "update {hidden} set __sum_1 = __sum_1 + 7; \
         update public.agg set total = total + 7; \
         insert into public.agg (g, h, total, n) values (99, null, 5, 1)"
    ))
    .await;
    fx.request_rebuild().await;
    let mut rebuilding = fx
        .rebuild_frozen(&[PausePoint::BetweenRebuildStructures])
        .await;
    rebuilding
        .reached(PausePoint::BetweenRebuildStructures)
        .await;
    fx.exec(
        "alter table public.items detach partition public.items_p1; \
         alter table public.items attach partition public.items_p1 for values from (4) to (8)",
    )
    .await;
    fx.d.release(&mut rebuilding, PausePoint::BetweenRebuildStructures)
        .await;
    rebuilding.finish().await;
    fx.d.settle().await;
    assert_eq!(fx.d.rows(wrong).await, Vec::<String>::new(), "rebuilt");
    assert_eq!(
        fx.d.rows("select count(*) from backfill_chunks").await,
        ["(0)"]
    );
    assert_eq!(fx.status().await, "live");
}

// --- A RESUME over a partitioned source: one transaction per leaf (#1101) ---

/// `RESUME` of the aggregate over a four-leaf source keeps the visible
/// target and rebuilds it one leaf at a time, then the groups no leaf
/// routes to: pause-era writes are built in, the rows of a group whose leaf
/// lost them are removed, and a target row no leaf backs goes with the
/// remainder.
#[tokio::test]
async fn a_resume_rebuilds_a_partitioned_source_per_leaf() {
    for kind in ["range", "list", "hash"] {
        let mut fx = Fx::start(&tree(kind), SEED).await;
        let id: i64 =
            fx.d.ctl
                .query_one(
                    "select id from transform_definitions where target_table = 'public.agg'",
                    &[],
                )
                .await
                .expect("id")
                .get(0);
        fx.exec(
            "update transform_definitions set status = 'paused' where target_table = 'public.agg'",
        )
        .await;
        fx.exec(
            "delete from public.items where g = 5; \
             update public.items set v = v + 1000 where g = 9; \
             insert into public.items values (1000, 14, 3); \
             insert into public.agg (g, total, n) values (99, 5, 1)",
        )
        .await;
        trellis::staging::quarantine::resume_transform(fx.d.pool(), "agg")
            .await
            .expect("resume");
        let pool = fx.d.pool().clone();
        build::start_ready_builds(&mut fx.d.ctl, &pool, &[id])
            .await
            .expect("start the resumed build");
        assert_eq!(fx.status().await, "backfilling", "{kind}");
        build::settle_builds(fx.d.pool()).await;
        fx.d.settle().await;
        fx.assert_right(&format!("{kind}: resumed")).await;
        assert_eq!(
            fx.d.rows("select count(*) from public.agg where g in (5, 99)")
                .await,
            ["(0)"],
            "{kind}: the groups no leaf backs are gone"
        );
    }
}

/// A second aggregate defined over the seeded source builds per leaf.
#[tokio::test]
async fn a_fresh_build_over_a_partitioned_source_with_rows_goes_per_leaf() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    let defs = [("id", int4()), ("g", int4()), ("v", int4())]
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    trellis::defs::install_definition(
        fx.d.pool(),
        "TRANSFORM agg2 FROM public.items GROUP BY g WITHOUT SOURCE INDEX SELECT COUNT(*) AS c, SUM(v) AS s",
        &defs,
        "public",
    )
    .await
    .expect("define a second aggregate");
    trellis::intake::harness::start_registered_builds(fx.d.pool())
        .await
        .expect("start its build");
    build::settle_builds(fx.d.pool()).await;
    fx.d.settle().await;
    assert_eq!(
        fx.d.rows("select g, c, s from public.agg2 order by g")
            .await,
        fx.d.rows("select g, count(*), sum(v) from public.items group by g order by g")
            .await
    );
    assert_eq!(
        fx.d.rows("select status from transform_definitions order by id")
            .await,
        ["(live)", "(live)"]
    );
}

/// A group-basis reader of an aggregate resumed over a partitioned source:
/// a page between two leaves' transactions, on a group of a leaf not yet
/// built, computes its prior image from the empty partials, so the reader
/// is rebuilt by the transaction that completes the whole build (the
/// remainder's), not by any leaf's, and converges.
#[tokio::test]
async fn a_reader_of_an_aggregate_resumed_per_leaf_is_rebuilt_once() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    let columns = [("g", int4()), ("total", int8()), ("n", int8())]
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    trellis::defs::install_definition(
        fx.d.pool(),
        "TRANSFORM roll FROM public.agg GROUP BY n WITHOUT SOURCE INDEX \
         SELECT COUNT(*) AS groups, SUM(total) AS t",
        &columns,
        "public",
    )
    .await
    .expect("define the reader");
    trellis::intake::harness::settle_builds(fx.d.pool()).await;
    fx.d.settle().await;
    build::settle_builds(fx.d.pool()).await;
    fx.d.settle().await;
    const ROLL: &str = "select n, groups, t from public.roll order by n";
    const ROLL_EXPECTED: &str = "select n, count(*), sum(total) from ( \
                                     select count(*) as n, sum(v) as total \
                                     from public.items group by g) s group by n order by n";
    assert_eq!(fx.d.rows(ROLL).await, fx.d.rows(ROLL_EXPECTED).await);
    let id: i64 =
        fx.d.ctl
            .query_one(
                "select id from transform_definitions where target_table = 'public.agg'",
                &[],
            )
            .await
            .expect("id")
            .get(0);
    fx.exec("update transform_definitions set status = 'paused' where target_table = 'public.agg'")
        .await;
    trellis::staging::quarantine::resume_transform(fx.d.pool(), "agg")
        .await
        .expect("resume");
    let pool = fx.d.pool().clone();
    build::start_ready_builds(&mut fx.d.ctl, &pool, &[id])
        .await
        .expect("start the resumed build");
    let mut building =
        fx.d.run_frozen(
            &[(PausePoint::BetweenRebuildStructures, "public.agg")],
            |pool| async move {
                let claimed = chunk_queue::claim_chunks_of(
                    &**pool.get().await.expect("pool"),
                    "build",
                    1,
                    &[chunk_queue::KIND_PLAN],
                )
                .await
                .expect("claim the plan job");
                assert_eq!(claimed.len(), 1, "one plan job to claim");
                build::run_claimed(&pool, &claimed[0], "build", &REBUILD_OPTIONS).await;
                Ok(())
            },
        )
        .await;
    building.reached(PausePoint::BetweenRebuildStructures).await;
    assert_eq!(
        fx.d.rows("select status from transform_definitions where target_table = 'public.roll'")
            .await,
        ["(live)"],
        "a leaf's transaction parks nothing"
    );
    // Group 13 is the last leaf's, not yet built: its page sees no partials.
    fx.exec("insert into public.items values (2013, 13, 1)")
        .await;
    let batch = fx.d.seal().await;
    fx.d.drain(batch, "page").await;
    fx.d.release(&mut building, PausePoint::BetweenRebuildStructures)
        .await;
    building.finish().await;
    assert_eq!(
        fx.d.rows("select status from transform_definitions where target_table = 'public.roll'")
            .await,
        ["(backfilling)"],
        "the build's last transaction rebuilds the reader"
    );
    build::settle_builds(fx.d.pool()).await;
    fx.d.settle().await;
    build::settle_builds(fx.d.pool()).await;
    fx.d.settle().await;
    fx.assert_right("resumed").await;
    assert_eq!(fx.d.rows(ROLL).await, fx.d.rows(ROLL_EXPECTED).await);
    assert_eq!(
        fx.d.rows("select status from transform_definitions order by id")
            .await,
        ["(live)", "(live)"]
    );
    assert_eq!(fx.d.deadlocks_logged(), Vec::<String>::new());
}

/// A leaf's correction that reads it by one `GROUP BY` matches the leaf's
/// groups to the locked ones at the hidden table's types: a `varchar` group
/// column is held as `text`, and its array match with the leaf's own
/// `varchar` column would have no `=` (#1318's review). One label per `g`,
/// so the oracle by `g` still checks every `(g, label)` group.
#[tokio::test]
async fn a_leaf_read_matches_a_varchar_group_column_at_its_held_type() {
    let schema = tree("list").replace(
        "v integer not null,",
        "label varchar(10) not null, v integer not null,",
    ) + "create index on public.items (g, label);";
    let d = Driver::start(
        &schema,
        &[
            ("id", int4()),
            ("g", int4()),
            ("label", ValueType::Text),
            ("v", int4()),
        ],
        &["TRANSFORM agg FROM public.items GROUP BY g, label \
          WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n"],
        &[SOURCE],
    )
    .await;
    let mut fx = Fx { d };
    fx.pass().await;
    fx.exec(
        "insert into public.items select i, i % 16, 'L' || (i % 16), i \
         from generate_series(1, 160) i",
    )
    .await;
    fx.d.settle().await;
    fx.assert_right("seeded").await;
    fx.exec("alter table public.items detach partition public.items_p3")
        .await;
    fx.correct().await;
    fx.assert_right("detached").await;
    fx.exec(
        "insert into public.items_p3 select 3000 + i, 13, 'L13', 2 from generate_series(1, 3) i; \
         alter table public.items attach partition public.items_p3 for values in (12, 13, 14, 15);",
    )
    .await;
    fx.pass().await;
    assert_eq!(fx.kinds().await, ["new_leaf"]);
    fx.run().await;
    fx.d.settle().await;
    fx.assert_right("attached").await;
}

/// `Trellis::status` reports a definition under correction as `backfilling`,
/// with a `correcting` entry for the node: its schema-qualified name and when
/// the pass queued the correction, with no count before it commits. Once it
/// commits the definition is `live` with nothing listed.
#[tokio::test]
async fn status_lists_the_node_under_correction_until_it_commits() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    let handle = Trellis::connect(
        Config::from_dsn(fx.d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    let status = handle.status("agg").await.expect("status").expect("agg");
    assert_eq!(
        (status.status, status.correcting),
        (trellis::TransformStatus::Live, Vec::new())
    );

    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    assert_eq!(fx.kinds().await, ["gone_node"]);
    // The staging worker's step that enqueues the correction's row.
    partition_correction::schedule(fx.d.pool())
        .await
        .expect("schedule");
    let queued_at: std::time::SystemTime =
        fx.d.ctl
            .query_one("select queued_at from partition_corrections", &[])
            .await
            .expect("the correction")
            .get(0);
    let status = handle.status("agg").await.expect("status").expect("agg");
    assert_eq!(status.status, trellis::TransformStatus::Backfilling);
    assert_eq!(
        status.correcting,
        vec![trellis::CorrectingNode {
            node: "public.items_p1".to_string(),
            since: queued_at,
            keys_remaining: None,
        }],
        "listed from the pass that queued it"
    );

    fx.run().await;
    fx.d.settle().await;
    let status = handle.status("agg").await.expect("status").expect("agg");
    assert_eq!(
        (status.status, status.correcting),
        (trellis::TransformStatus::Live, Vec::new())
    );
    fx.assert_right("dropped").await;
}
