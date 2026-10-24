//! #958 (X12-7d, 7a): renames between passes, and a three-level tree, on a
//! partitioned source under a group-basis aggregate.
//!
//! A leaf or middle `RENAME` and `SET SCHEMA`, and a `RENAME COLUMN` of a
//! partition-key column, each between two reconcile passes: the recorded
//! node names and routing texts go stale, and a write must not be lost nor a
//! later operation on the renamed node left uncorrected. Same driver and
//! oracle as `tests/partition_group_basis.rs`; nothing waits for
//! convergence (#297).

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
        trellis::dev::staging::set_group_basis(true);
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
}

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

/// The test shape: renames, writes through the parent and direct to the
/// renamed leaf, then a lifecycle operation on the renamed node.
#[tokio::test]
async fn a_renamed_leaf_is_still_captured_and_corrected() {
    for (what, rename, name) in [
        (
            "rename",
            "alter table public.items_p1 rename to items_r1",
            "public.items_r1",
        ),
        (
            "set schema",
            "create schema other; alter table public.items_p1 set schema other",
            "other.items_p1",
        ),
    ] {
        for op in ["drop table", "truncate"] {
            let mut fx = Fx::start(&tree("range"), SEED).await;
            fx.exec(rename).await;
            fx.pass().await;
            fx.exec(&format!(
                "insert into public.items values (9001, 5, 10), (9002, 6, 20); \
                 insert into {name} values (9003, 7, 30); \
                 update public.items set v = v + 1 where g = 4; \
                 delete from public.items where g = 5 and id % 2 = 0"
            ))
            .await;
            fx.d.settle().await;
            fx.assert_right(&format!("{what}: written")).await;
            fx.exec(&format!("{op} {name}")).await;
            fx.correct().await;
            fx.correct().await;
            fx.assert_right(&format!("{what}/{op}")).await;
            assert!(fx.queue().await.is_empty(), "{what}/{op}");
        }
    }
}

/// A loaded leaf attached, or a new leaf written directly before capture
/// reaches it, holding a group no target row or ring image has: only a read
/// of the node finds that group. Its correction is queued under the leaf's
/// name, then the leaf is renamed or moved to another schema before the
/// engine runs. The engine reads the node by oid, not by the queued name, so
/// the group is still counted.
#[tokio::test]
async fn a_leaf_renamed_after_its_correction_is_queued_is_still_read() {
    for (what, between) in [
        ("rename", "alter table public.items_x rename to items_y"),
        (
            "set schema",
            "create schema other; alter table public.items_x set schema other",
        ),
        ("no rename", ""),
    ] {
        for add in [
            "create table public.items_x (like public.items including all); \
             insert into public.items_x values (9100, 17, 100), (9101, 18, 200); \
             alter table public.items attach partition public.items_x for values from (16) to (20)",
            "create table public.items_x partition of public.items for values from (16) to (20); \
             insert into public.items_x values (9100, 17, 100), (9101, 18, 200)",
        ] {
            let mut fx = Fx::start(&tree("range"), SEED).await;
            fx.exec(add).await;
            fx.pass().await;
            assert!(
                !fx.queue().await.is_empty(),
                "{what}: the new leaf is queued"
            );
            fx.exec(between).await;
            fx.correct().await;
            fx.correct().await;
            fx.assert_right(&format!("{what}: {add}")).await;
            assert!(fx.queue().await.is_empty(), "{what}");
        }
    }
}

/// A middle node renamed, then one of its leaves dropped and the middle
/// truncated.
#[tokio::test]
async fn a_renamed_middle_is_still_captured_and_corrected() {
    for op in [
        "drop table public.items_mm1",
        "truncate public.items_mm",
        "drop table public.items_mm",
    ] {
        let mut fx = Fx::start(&two_level(), SEED).await;
        fx.exec(
            "alter table public.items_m rename to items_mm; \
                 alter table public.items_m1 rename to items_mm1",
        )
        .await;
        fx.pass().await;
        assert!(fx.queue().await.is_empty(), "{op}: a rename queues nothing");
        fx.exec(
            "insert into public.items select 9000 + g, g, 1 from generate_series(8, 15) g; \
             update public.items_mm1 set v = v + 5",
        )
        .await;
        fx.d.settle().await;
        fx.assert_right(&format!("{op}: written")).await;
        fx.exec(op).await;
        fx.correct().await;
        fx.correct().await;
        fx.assert_right(op).await;
        assert!(fx.queue().await.is_empty(), "{op}");
    }
}

/// `RENAME COLUMN` of the partition-key column: every recorded routing text
/// names the old column and no lifecycle fingerprint changes, so the pass
/// queues nothing. The column check (#622) pauses the reader instead, and a
/// later pass leaves it paused. The resumed rebuild is
/// `partition_lifecycle_ledger.rs`'s: a group-basis fresh build over rows is
/// #1101.
#[tokio::test]
async fn a_renamed_partition_key_column_pauses_the_reader_and_queues_nothing() {
    let mut fx = Fx::start(&tree("range"), SEED).await;
    fx.exec("alter table public.items rename column g to gg")
        .await;
    fx.pass().await;
    assert_eq!(fx.status().await, "paused");
    assert!(
        fx.queue().await.is_empty(),
        "a renamed column is no tree change"
    );
    let failures =
        fx.d.rows("select error from capture_failures where source_table = 'public.items'")
            .await;
    assert!(
        failures
            .iter()
            .any(|f| f.contains("\"g\"") && f.contains("renamed")),
        "{failures:?}"
    );
    fx.exec("drop table public.items_p1").await;
    fx.pass().await;
    assert_eq!(fx.status().await, "paused", "a later pass leaves it paused");
}

/// `LIST`, then `RANGE`, then a `DEFAULT` at the middle and one at the root:
/// groups 0-7 under `items_a`, 8-15 under `items_b`, anything else in the
/// root's `DEFAULT`. The `DEFAULT` of each middle holds the groups its
/// ranges leave out (4-7 and 12-15).
fn three_level() -> String {
    format!(
        "{} create table public.items_a partition of public.items \
             for values in (0, 1, 2, 3, 4, 5, 6, 7) partition by range (g); \
         create table public.items_a0 partition of public.items_a for values from (0) to (4); \
         create table public.items_ad partition of public.items_a default; \
         create table public.items_b partition of public.items \
             for values in (8, 9, 10, 11, 12, 13, 14, 15) partition by range (g); \
         create table public.items_b0 partition of public.items_b for values from (8) to (12); \
         create table public.items_bd partition of public.items_b default; \
         create table public.items_d partition of public.items default;",
        head("list")
    )
}

const SEED_24: &str = "insert into public.items select i, i % 24, i from generate_series(1, 240) i";

/// X12-7a, 7b: the lifecycle operations on a tree whose middle nodes have a
/// `DEFAULT` leaf and whose root has one, each operating on a leaf, a
/// `DEFAULT`, or a middle node.
#[tokio::test]
async fn every_operation_on_a_list_range_default_tree_converges() {
    for op in [
        "truncate public.items_ad",
        "truncate public.items_a",
        "truncate public.items_d",
        "drop table public.items_bd",
        "drop table public.items_b",
        "drop table public.items_d",
        "alter table public.items_a detach partition public.items_ad",
        "alter table public.items detach partition public.items_b",
        "alter table public.items detach partition public.items_d",
        // A loaded DEFAULT attached to a middle node, and to the root.
        "alter table public.items_a detach partition public.items_ad; \
         insert into public.items_ad values (8000, 5, 1), (8001, 6, 2); \
         alter table public.items_a attach partition public.items_ad default",
        "alter table public.items detach partition public.items_d; \
         insert into public.items_d values (8100, 30, 1); \
         alter table public.items attach partition public.items_d default",
        // A new leaf beside a DEFAULT takes rows from it, written directly
        // first.
        "alter table public.items_a detach partition public.items_ad; \
         create table public.items_a1 partition of public.items_a for values from (4) to (6); \
         insert into public.items_a1 select id, g, v from public.items_ad where g in (4, 5); \
         delete from public.items_ad where g in (4, 5); \
         insert into public.items_a1 values (8200, 4, 3), (8201, 5, 4); \
         alter table public.items_a attach partition public.items_ad default",
    ] {
        let mut fx = Fx::start(&three_level(), SEED_24).await;
        fx.exec(op).await;
        fx.correct().await;
        fx.correct().await;
        fx.assert_right(op).await;
        assert!(fx.queue().await.is_empty(), "{op}");
    }
}

/// X12-7a: an interrupted `DETACH … CONCURRENTLY` on a two-level tree, of a
/// leaf under the `HASH` middle and of the middle itself, with a write
/// direct to the pending node and one through the parent beside it. The
/// correction waits while the detach is pending and runs once it is
/// finalized.
#[tokio::test]
async fn an_interrupted_concurrent_detach_on_a_two_level_tree_converges() {
    for (what, parent, node) in [
        ("leaf", "public.items_m", "public.items_m2"),
        ("middle", "public.items", "public.items_m"),
    ] {
        let mut fx = Fx::start(&two_level(), SEED).await;
        let holder = drain_driver::connect(fx.d.db.dsn()).await;
        holder
            .batch_execute(&format!("begin; lock table {node} in access share mode"))
            .await
            .expect("hold the node");
        let detacher = drain_driver::connect(fx.d.db.dsn()).await;
        detacher
            .batch_execute("set statement_timeout = '1500ms'")
            .await
            .expect("timeout");
        let interrupted = detacher
            .batch_execute(&format!(
                "alter table {parent} detach partition {node} concurrently"
            ))
            .await;
        assert!(interrupted.is_err(), "{what}: the second phase waits");
        // A write direct to a leaf of the pending node, and one beside it.
        fx.exec(&format!(
            "update {} set v = v + 1000; \
             update public.items_lo set v = v + 7",
            if what == "leaf" {
                node
            } else {
                "public.items_m0"
            }
        ))
        .await;
        fx.pass().await;
        assert_eq!(fx.kinds().await, ["gone_node"], "{what}");
        let waiting = fx.run().await;
        assert_eq!((waiting.completed, waiting.pages), (0, 0), "{what}");
        holder.batch_execute("rollback").await.expect("release");
        fx.exec(&format!(
            "alter table {parent} detach partition {node} finalize"
        ))
        .await;
        fx.correct().await;
        fx.correct().await;
        fx.assert_right(what).await;
        assert!(fx.queue().await.is_empty(), "{what}");
    }
}
