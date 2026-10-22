//! #1111 (J6): partition maintenance under a `JOIN` transform whose fact
//! side (`FROM`) or dimension side (the joined table) is a partitioned root
//! (ADR-0017, ADR-0019 "Either side may be partitioned when its partial key
//! holds every partition-key column").
//!
//! A leaf or middle `DROP`, `DETACH`, `TRUNCATE` or `ATTACH`, a new leaf
//! written directly before its capture, a `DEFAULT` whose siblings changed,
//! and the complement sweep's correction are each corrected by recomputing
//! the side's partial cells (`P` or `M`) that route to the node, through the
//! `J`-locked writer, which carries each cell's change into `T`.
//!
//! Every test turns the group-basis dev knob on before its driver starts (a
//! `JOIN` applies only behind it) and starts from two empty tables, since a
//! `JOIN` build of a partitioned side with rows waits on #1286: it goes `live`
//! with empty hidden tables and the seed arrives through capture. The pass,
//! the seal, the drain and the correction engine are called by hand; nothing waits for convergence
//! (#297). The oracle is each definition's own `JOIN … GROUP BY`, and the
//! hidden `P`, `M`, `J` and `T` against `GROUP BY`s of the tables.
//!
//! The cases are #1043 E1's variant-L scenarios (`P.*`) with a `JOIN` side
//! operated on, and #958's lifecycle cells (X12-7a two-level trees, X12-7b a
//! `DEFAULT` operated on, X12-7c a `HASH` re-split, X12-7e writes into the
//! node while it is corrected).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::capture::partitions::{self, PendingCorrection};
use trellis::capture::reconcile::PassOutcome;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;
use trellis::staging::partition_correction::{self, Options, Outcome};
use trellis::staging::self_check::{SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page};

/// Which table is the partitioned root.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Part {
    /// `a`, the `FROM` (fact) side: `P` is corrected.
    Fact,
    /// `b`, the joined (dimension) side: `M` and `J.matches` are corrected.
    Dim,
}

impl Part {
    fn table(self) -> &'static str {
        match self {
            Part::Fact => "public.a",
            Part::Dim => "public.b",
        }
    }

    /// The bare name of the partitioned table, which its leaves' names
    /// start with.
    fn name(self) -> &'static str {
        match self {
            Part::Fact => "a",
            Part::Dim => "b",
        }
    }
}

const BOTH: [Part; 2] = [Part::Fact, Part::Dim];

/// A `LEFT JOIN` grouped by the dimension's column alone, so `P` is keyed
/// by the join key; and an inner `JOIN` grouped by a column of each side.
/// Both partial keys hold `k`, the partition key.
const DEFINITIONS: [&str; 2] = [
    "TRANSFORM l FROM public.a LEFT JOIN public.b ON a.k = b.k GROUP BY b.gb \
     SELECT b.gb AS gb, COUNT(*) AS n, COUNT(a.x) AS cx, SUM(a.x) AS sx, \
            COUNT(b.y) AS cy, SUM(b.y) AS sy",
    "TRANSFORM i FROM public.a JOIN public.b ON a.k = b.k GROUP BY a.ga, b.gb \
     SELECT a.ga AS ga, b.gb AS gb, COUNT(*) AS n, SUM(a.x) AS sx, SUM(b.y) AS sy",
];

/// Each target and its oracle, both ordered by its group columns.
const ORACLES: [(&str, &str, &str); 2] = [
    (
        "l",
        "select gb, n, cx, sx, cy, sy from public.l order by gb",
        "select b.gb, count(*), count(a.x), sum(a.x), count(b.y), sum(b.y) \
         from public.a left join public.b on a.k = b.k group by b.gb order by b.gb",
    ),
    (
        "i",
        "select ga, gb, n, sx, sy from public.i order by ga, gb",
        "select a.ga, b.gb, count(*), sum(a.x), sum(b.y) \
         from public.a join public.b on a.k = b.k group by a.ga, b.gb order by a.ga, b.gb",
    ),
];

/// The columns of `a`, or of `b`, after `id`.
fn columns(part: Part) -> &'static str {
    match part {
        Part::Fact => "ga integer, k integer not null, x integer",
        Part::Dim => "k integer not null, gb integer, y integer",
    }
}

/// The unpartitioned other table.
fn plain(part: Part) -> String {
    match part {
        Part::Fact => "create table public.b (id integer primary key, k integer, gb integer, \
                       y integer); create index on public.b (k);"
            .to_string(),
        Part::Dim => "create table public.a (id integer primary key, ga integer, k integer, \
                      x integer); create index on public.a (k);"
            .to_string(),
    }
}

/// `part`'s table partitioned `kind` on `k`, with no leaves, and the other
/// table plain.
fn head(part: Part, kind: &str) -> String {
    format!(
        "{} create table {} (id integer not null, {}, primary key (k, id)) \
         partition by {kind} (k);",
        plain(part),
        part.table(),
        columns(part)
    )
}

/// Four leaves `<t>_p0`..`<t>_p3` for the keys 0-3, 4-7, 8-11 and 12-15
/// (`HASH`: remainders 0-3 of modulus 4), empty.
fn tree(part: Part, kind: &str) -> String {
    let (t, n) = (part.table(), part.name());
    let leaves = match kind {
        "hash" => (0..4)
            .map(|r| {
                format!(
                    "create table public.{n}_p{r} partition of {t} \
                     for values with (modulus 4, remainder {r});"
                )
            })
            .collect::<String>(),
        "range" => (0..4)
            .map(|i| {
                format!(
                    "create table public.{n}_p{i} partition of {t} for values from ({}) to ({});",
                    i * 4,
                    i * 4 + 4
                )
            })
            .collect::<String>(),
        _ => (0..4)
            .map(|i| {
                format!(
                    "create table public.{n}_p{i} partition of {t} \
                     for values in ({}, {}, {}, {});",
                    i * 4,
                    i * 4 + 1,
                    i * 4 + 2,
                    i * 4 + 3
                )
            })
            .collect::<String>(),
    };
    format!("{}{leaves}", head(part, kind))
}

/// The dimension: two rows for each key 0-13 (keys 14 and 15 match
/// nothing), `gb` 0-2. The facts: ten for each key 0-15, `ga` 0-3.
const SEED: &str = "insert into public.b select i, i % 14, i % 3, i from generate_series(1, 28) i; \
                    insert into public.a select i, i % 4, i % 16, i from generate_series(1, 160) i";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

struct Fx {
    d: Driver,
    part: Part,
}

impl Fx {
    /// `schema` with both `JOIN`s live over its empty tables, `seed` written
    /// through capture and applied, and the tree recorded.
    async fn start(part: Part, schema: &str, seed: &str) -> Fx {
        trellis::dev::staging::set_group_basis(true);
        trellis::defs::partition_tree::open_gate_for_tests();
        let d = Driver::start(
            schema,
            &[("id", int4()), ("ga", int4()), ("k", int4()), ("x", int4())],
            &DEFINITIONS,
            &["public.a", "public.b"],
        )
        .await;
        let mut fx = Fx { d, part };
        // The first sight of the source records its tree and queues nothing.
        fx.pass().await;
        assert!(fx.queue().await.is_empty());
        fx.exec(seed).await;
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
        partitions::pending_corrections(&self.d.ctl, self.part.table())
            .await
            .expect("the queue")
    }

    async fn kinds(&self) -> Vec<String> {
        self.queue().await.into_iter().map(|c| c.kind).collect()
    }

    /// Each target whose rows differ from its `JOIN … GROUP BY`, with both.
    async fn wrong_targets(&self) -> Vec<String> {
        let mut wrong = Vec::new();
        for (name, actual, expected) in ORACLES {
            let (have, want) = (self.d.rows(actual).await, self.d.rows(expected).await);
            if have != want {
                wrong.push(format!("{name}: have {have:?}, want {want:?}"));
            }
        }
        wrong
    }

    /// `self_check` of every target; each must report `Converged`.
    async fn divergences(&self) -> Vec<String> {
        let mut found = Vec::new();
        for (name, _, _) in ORACLES {
            let mut after = None;
            loop {
                let page = check_page(
                    self.d.pool(),
                    name,
                    SelfCheckScope { after, limit: 1000 },
                    SelfCheckMode::Standard,
                    Duration::from_secs(30),
                )
                .await
                .expect("a page of the check");
                match page.outcome {
                    SelfCheckOutcome::Converged => {}
                    other => found.push(format!("{name}: {other:?}")),
                }
                after = page.next_after;
                if after.is_none() {
                    break;
                }
            }
        }
        found
    }

    /// The hidden `P`, `M`, `J` and `T` of each definition against what the
    /// tables hold, each row tagged with the table and `have` or `want`. A
    /// zero cell (or `J` row) is compared only when a partial of it is not
    /// zero.
    async fn hidden_wrong(&self) -> Vec<String> {
        // Each definition's `P` key, `T` key, `T`'s key as the oracle reads
        // it, and its join.
        const SHAPES: [(&str, &str, &str, &str, &str); 2] = [
            ("l", "k", "gb", "b.gb", "left join"),
            ("i", "ga, k", "ga, gb", "a.ga, b.gb", "join"),
        ];
        let mut out = Vec::new();
        for (name, p_key, t_key, t_oracle_key, join) in SHAPES {
            let id: i64 = self
                .d
                .ctl
                .query_one(
                    "select id from transform_definitions where target_table = $1",
                    &[&format!("public.{name}")],
                )
                .await
                .expect("definition id")
                .get(0);
            let hidden = |part: &str| format!("{DEFAULT_SCHEMA}.join_{id}_{part}");
            let checks = [
                (
                    "p",
                    format!(
                        "select {p_key}, __n, __count_1, __sum_1 from {} \
                         where __n <> 0 or __count_1 <> 0 or __sum_1 <> 0",
                        hidden("p")
                    ),
                    format!(
                        "select {p_key}, count(*), count(x), coalesce(sum(x), 0) \
                         from public.a group by {p_key}"
                    ),
                ),
                (
                    "m",
                    format!(
                        "select gb, k, __n, __count_1, __sum_1 from {} \
                         where __n <> 0 or __count_1 <> 0 or __sum_1 <> 0",
                        hidden("m")
                    ),
                    "select gb, k, count(*), count(y), coalesce(sum(y), 0) \
                     from public.b where k is not null group by gb, k"
                        .to_string(),
                ),
                (
                    "j",
                    format!(
                        "select k, __matches from {} where __matches <> 0",
                        hidden("j")
                    ),
                    "select k, count(*) from public.b where k is not null group by k".to_string(),
                ),
                (
                    "t",
                    format!(
                        "select {t_key}, __n, __count_1, __sum_1, __count_2, __sum_2 from {} \
                         where __n <> 0 or __count_1 <> 0 or __sum_1 <> 0 \
                            or __count_2 <> 0 or __sum_2 <> 0",
                        hidden("t")
                    ),
                    format!(
                        "select {t_oracle_key}, count(*), count(a.x), coalesce(sum(a.x), 0), \
                                count(b.y), coalesce(sum(b.y), 0) \
                         from public.a {join} public.b on a.k = b.k group by {t_oracle_key}"
                    ),
                ),
            ];
            for (part, have, want) in checks {
                out.extend(
                    self.d
                        .rows(&format!(
                            "select '{name}.{part} have' as what, h::text as cell from \
                                 (({have}) except all ({want})) h \
                             union all \
                             select '{name}.{part} want', o::text from \
                                 (({want}) except all ({have})) o"
                        ))
                        .await,
                );
            }
        }
        out
    }

    /// Hidden partials a delta drove below zero.
    async fn negative_partials(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (name, _, _) in ORACLES {
            let id: i64 = self
                .d
                .ctl
                .query_one(
                    "select id from transform_definitions where target_table = $1",
                    &[&format!("public.{name}")],
                )
                .await
                .expect("definition id")
                .get(0);
            for part in ["p", "m", "t"] {
                out.extend(
                    self.d
                        .rows(&format!(
                            "select '{name}.{part}', h::text from {DEFAULT_SCHEMA}.join_{id}_{part} h \
                             where __n < 0"
                        ))
                        .await,
                );
            }
            out.extend(
                self.d
                    .rows(&format!(
                        "select '{name}.j', h::text from {DEFAULT_SCHEMA}.join_{id}_j h \
                         where __matches < 0"
                    ))
                    .await,
            );
        }
        out
    }

    async fn statuses(&self) -> Vec<String> {
        self.d
            .rows("select target_table, status from transform_definitions order by id")
            .await
    }

    /// Every target equals its oracle, `self_check` finds nothing, the hidden tables match the tables, no partial is
    /// negative, and both definitions are `live`.
    async fn assert_right(&self, what: &str) {
        assert_eq!(self.wrong_targets().await, Vec::<String>::new(), "{what}");
        assert_eq!(self.divergences().await, Vec::<String>::new(), "{what}");
        assert_eq!(self.hidden_wrong().await, Vec::<String>::new(), "{what}");
        assert_eq!(
            self.negative_partials().await,
            Vec::<String>::new(),
            "{what}"
        );
        assert_eq!(
            self.statuses().await,
            ["(public.l,live)", "(public.i,live)"],
            "{what}"
        );
    }

    /// Runs the engine to its end, with no drain before it: a ring change
    /// staged before the operation is still undrained.
    async fn run(&self) -> Outcome {
        let outcome = partition_correction::run_pending(self.d.pool(), &Options::default())
            .await
            .expect("run the pending corrections");
        if outcome.remaining == 0 {
            assert_eq!(
                self.statuses().await,
                ["(public.l,live)", "(public.i,live)"],
                "the correction's completion moves each definition back to live"
            );
        }
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

    /// Two passes, each followed by the engine and a drain: the second finds
    /// what the first pass's corrections left (the sweep's convergence).
    async fn converge_twice(&mut self, what: &str) {
        assert!(
            !self.wrong_targets().await.is_empty(),
            "{what}: rows to correct"
        );
        self.correct().await;
        self.correct().await;
        self.assert_right(what).await;
        assert!(self.queue().await.is_empty(), "{what}");
    }
}

// --- Leaf and middle operations (#1043 E1 variant L, `P.*`) ---------------

/// `P.drop` / `P.detach`: a leaf of either side dropped or detached with
/// changes to it still undrained in the ring. The recompute's basis sees
/// their transactions, so their pages are refused after it.
#[tokio::test]
async fn a_dropped_or_detached_leaf_with_undrained_changes_is_recomputed() {
    for part in BOTH {
        for kind in ["hash", "range", "list"] {
            for op in [
                "drop table public.{n}_p1",
                "alter table {t} detach partition public.{n}_p1",
            ] {
                let op = op.replace("{n}", part.name()).replace("{t}", part.table());
                let what = format!("{part:?} {kind}: {op}");
                let mut fx = Fx::start(part, &tree(part, kind), SEED).await;
                let n = part.name();
                // Undrained: an insert, an update and a delete in the leaf.
                fx.exec(&format!(
                    "insert into {t} (id, k, {c}) select 500 + i, k, 1 from \
                         (select distinct k from public.{n}_p1) s(k), generate_series(1, 2) i; \
                     update public.{n}_p1 set {c} = {c} + 100 where id % 3 = 0; \
                     delete from public.{n}_p1 where id % 5 = 0;",
                    t = part.table(),
                    c = if part == Part::Fact { "x" } else { "y" },
                ))
                .await;
                fx.exec(&op).await;
                fx.pass().await;
                assert_eq!(fx.kinds().await, ["gone_node"], "{what}");
                let outcome = fx.run().await;
                assert_eq!(outcome.completed, 1, "{what}");
                assert!(fx.queue().await.is_empty(), "{what}");
                fx.d.settle().await;
                fx.assert_right(&what).await;
                if op.starts_with("alter") {
                    // A write to the detached table stages nothing.
                    fx.exec(&format!(
                        "insert into public.{n}_p1 (id, k) select 900 + i, k from \
                             (select distinct k from public.{n}_p1) s(k), generate_series(1, 2) i"
                    ))
                    .await;
                    fx.d.settle().await;
                    fx.assert_right(&what).await;
                }
            }
        }
    }
}

/// `P.stale-new-group`: an undrained insert creates a cell no hidden table
/// has yet, in the leaf that is then dropped. The correction's set takes
/// the cells of undrained ring images, so the stale page is refused rather
/// than creating the cell with a row the drop removed.
#[tokio::test]
async fn an_undrained_change_naming_a_new_cell_of_a_dropped_leaf_is_refused() {
    for part in BOTH {
        let mut fx = Fx::start(part, &tree(part, "range"), SEED).await;
        // Key 6 gets a new `gb` (or `ga`) only in the ring.
        let insert = match part {
            Part::Fact => "insert into public.a values (1000, 77, 6, 4)",
            Part::Dim => "insert into public.b values (1000, 6, 77, 4)",
        };
        fx.exec(insert).await;
        fx.exec(&format!("drop table public.{}_p1", part.name()))
            .await;
        fx.pass().await;
        fx.run().await;
        fx.d.settle().await;
        fx.assert_right(&format!("{part:?}")).await;
    }
}

/// `P.truncate-leaf`: a leaf truncated with undrained changes, then an
/// insert through the parent into another leaf. The seal queues the
/// correction from the truncate's marker; the other leaves' cells are
/// untouched.
#[tokio::test]
async fn a_truncated_leaf_with_undrained_changes_is_recomputed() {
    for part in BOTH {
        for kind in ["hash", "range", "list"] {
            let what = format!("{part:?} {kind}");
            let (t, n) = (part.table(), part.name());
            let mut fx = Fx::start(part, &tree(part, kind), SEED).await;
            fx.exec(&format!(
                "insert into {t} (id, k) select 500 + i, k from \
                     (select distinct k from public.{n}_p2) s(k), generate_series(1, 2) i; \
                 truncate public.{n}_p2;"
            ))
            .await;
            let batch = fx.d.seal().await;
            assert_eq!(fx.kinds().await, ["truncated"], "{what}");
            fx.exec(&format!(
                "insert into {t} (id, k) select 700 + i, k from \
                     (select distinct k from public.{n}_p0) s(k), generate_series(1, 1) i"
            ))
            .await;
            fx.run().await;
            fx.drain(batch).await;
            fx.assert_right(&what).await;
            assert!(fx.queue().await.is_empty(), "{what}");
        }
    }
}

/// `P.truncate-insert-txn`: `TRUNCATE` a leaf and `INSERT` into it in one
/// transaction. A zero of the leaf's cells would erase the new row, whose
/// change its basis then refuses; the recompute counts it.
#[tokio::test]
async fn a_truncate_and_an_insert_into_the_leaf_in_one_transaction_keep_the_new_row() {
    for part in BOTH {
        let (t, n) = (part.table(), part.name());
        let mut fx = Fx::start(part, &tree(part, "range"), SEED).await;
        let values = match part {
            Part::Fact => ["(1000, 1, 3, 99)", "(1001, 2, 2, 98)"],
            Part::Dim => ["(1000, 3, 1, 99)", "(1001, 2, 2, 98)"],
        };
        fx.exec(&format!(
            "begin; truncate public.{n}_p0; \
             insert into public.{n}_p0 values {}; \
             insert into {t} values {}; commit;",
            values[0], values[1]
        ))
        .await;
        let batch = fx.d.seal().await;
        assert_eq!(fx.kinds().await, ["truncated"]);
        fx.run().await;
        fx.drain(batch).await;
        fx.assert_right(&format!("{part:?} truncate and insert"))
            .await;
    }
}

/// A middle `TRUNCATE` corrects every cell under the middle through its own
/// text, and leaves the other leaves' cells alone.
#[tokio::test]
async fn a_truncated_middle_is_recomputed_through_its_own_text() {
    for part in BOTH {
        let (t, n) = (part.table(), part.name());
        let mut fx = Fx::start(
            part,
            &format!(
                "{} create table public.{n}_m partition of {t} for values from (0) to (8) \
                     partition by list (k); \
                 create table public.{n}_m0 partition of public.{n}_m for values in (0, 1, 2, 3); \
                 create table public.{n}_m1 partition of public.{n}_m for values in (4, 5, 6, 7); \
                 create table public.{n}_p2 partition of {t} for values from (8) to (16);",
                head(part, "range")
            ),
            SEED,
        )
        .await;
        fx.exec(&format!("truncate public.{n}_m")).await;
        let batch = fx.d.seal().await;
        assert_eq!(fx.kinds().await, ["truncated"]);
        fx.run().await;
        fx.drain(batch).await;
        fx.assert_right(&format!("{part:?} middle truncate")).await;
    }
}

/// A root `TRUNCATE` of a partitioned side clears what a `TRUNCATE` of that
/// side clears, once, and the leaves' own markers then correct nothing.
#[tokio::test]
async fn a_root_truncate_clears_the_side() {
    for part in BOTH {
        let mut fx = Fx::start(part, &tree(part, "hash"), SEED).await;
        fx.exec(&format!("truncate {}", part.table())).await;
        fx.correct().await;
        fx.correct().await;
        fx.assert_right(&format!("{part:?} root truncate")).await;
        assert!(fx.queue().await.is_empty());
    }
}

/// `P.drop-recreate-txn`: a leaf dropped and re-created with rows in one
/// transaction; the new table's rows were written before any capture.
#[tokio::test]
async fn a_leaf_dropped_and_recreated_with_rows_in_one_transaction_is_recomputed() {
    for part in BOTH {
        let (t, n) = (part.table(), part.name());
        let mut fx = Fx::start(part, &tree(part, "range"), SEED).await;
        fx.exec(&format!(
            "begin; drop table public.{n}_p1; \
             create table public.{n}_p1 partition of {t} for values from (4) to (8); \
             insert into public.{n}_p1 (id, k) select 2000 + i, 4 + i % 3 from generate_series(1, 9) i; \
             commit;"
        ))
        .await;
        fx.correct().await;
        fx.assert_right(&format!("{part:?} drop and re-create"))
            .await;
    }
}

/// `P.attach`: a loaded table attached as a leaf, with direct writes before
/// its capture, a write through the parent, and a direct write between the
/// pass that installs its capture and the correction.
#[tokio::test]
async fn an_attached_leaf_with_rows_is_recomputed() {
    for part in BOTH {
        for kind in ["hash", "range", "list"] {
            let what = format!("{part:?} {kind}");
            let (t, n) = (part.table(), part.name());
            let mut fx = Fx::start(part, &tree(part, kind), SEED).await;
            fx.exec(&format!("alter table {t} detach partition public.{n}_p3"))
                .await;
            fx.correct().await;
            fx.assert_right(&what).await;
            let bound = match kind {
                "hash" => "for values with (modulus 4, remainder 3)",
                "range" => "for values from (12) to (16)",
                _ => "for values in (12, 13, 14, 15)",
            };
            // Uncaptured writes to the detached table, then the attach.
            fx.exec(&format!(
                "insert into public.{n}_p3 (id, k) select 3000 + i, k from \
                     (select distinct k from public.{n}_p3) s(k), generate_series(1, 3) i; \
                 alter table {t} attach partition public.{n}_p3 {bound}; \
                 insert into {t} (id, k) select 4000 + i, k from \
                     (select distinct k from public.{n}_p3) s(k), generate_series(1, 2) i; \
                 insert into public.{n}_p3 (id, k) select 4500 + i, k from \
                     (select distinct k from public.{n}_p3) s(k), generate_series(1, 2) i;"
            ))
            .await;
            fx.pass().await;
            assert_eq!(fx.kinds().await, ["new_leaf"], "{what}");
            // Captured now: a direct write between the pass and the correction.
            fx.exec(&format!(
                "insert into public.{n}_p3 (id, k) select 4800 + i, k from \
                     (select distinct k from public.{n}_p3) s(k), generate_series(1, 1) i"
            ))
            .await;
            fx.run().await;
            fx.d.settle().await;
            fx.assert_right(&what).await;
        }
    }
}

/// A leaf created and written directly before its capture exists: only its
/// correction's read of the node's rows counts those rows.
#[tokio::test]
async fn a_new_leaf_with_direct_writes_before_its_capture_is_recomputed() {
    for part in BOTH {
        let (t, n) = (part.table(), part.name());
        let mut fx = Fx::start(
            part,
            &format!(
                "{} create table public.{n}_p0 partition of {t} for values from (0) to (8);",
                head(part, "range")
            ),
            // The partitioned table's keys 0-7 only, the other's 0-15.
            &match part {
                Part::Fact => SEED.replace("i % 16", "i % 8"),
                Part::Dim => SEED.replace("i % 14", "i % 8"),
            },
        )
        .await;
        fx.exec(&format!(
            "create table public.{n}_p1 partition of {t} for values from (8) to (16); \
             insert into public.{n}_p1 (id, k) select 5000 + i, 8 + i % 8 from generate_series(1, 40) i;"
        ))
        .await;
        assert!(
            !fx.wrong_targets().await.is_empty(),
            "{part:?}: the direct rows are missed"
        );
        fx.correct().await;
        fx.assert_right(&format!("{part:?} new leaf")).await;
    }
}

/// X12-7e: writes into the leaf land while its correction holds the
/// recomputed cells: one committed before the recompute's snapshot is
/// counted by it and refused by its basis; one after it applies.
#[tokio::test]
async fn writes_into_the_leaf_during_its_correction_converge() {
    for part in BOTH {
        let (t, n) = (part.table(), part.name());
        let mut fx = Fx::start(part, &tree(part, "hash"), SEED).await;
        let writer = fx.d.user().await;
        fx.exec(&format!("truncate public.{n}_p1")).await;
        let batch = fx.d.seal().await;
        assert_eq!(fx.kinds().await, ["truncated"]);
        let keys: Vec<i32> =
            fx.d.ctl
                .query(
                    &format!(
                        "select k from generate_series(0, 15) k \
                         where satisfies_hash_partition('{t}'::regclass, 4, 1, k) order by k"
                    ),
                    &[],
                )
                .await
                .expect("leaf keys")
                .iter()
                .map(|r| r.get(0))
                .collect();
        // The first definition's correction (`l`) is frozen; `i`'s runs after.
        let mut run =
            fx.d.run_frozen(
                &[
                    (PausePoint::BeforeCorrectionRecompute, "public.l"),
                    (PausePoint::BeforeCorrectionCommit, "public.l"),
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
                &format!("insert into {t} (id, k) values (6000, $1)"),
                &[&keys[0]],
            )
            .await
            .expect("write before the recompute");
        fx.d.release(&mut run, PausePoint::BeforeCorrectionRecompute)
            .await;
        run.reached(PausePoint::BeforeCorrectionCommit).await;
        // While the recompute holds its cells: an insert into another key
        // and an update of the row written above, both committed after its
        // snapshot.
        let c = if part == Part::Fact { "x" } else { "y" };
        writer
            .batch_execute(&format!(
                "insert into {t} (id, k, {c}) values (6001, {}, 12); \
                 update {t} set {c} = coalesce({c}, 0) + 1 where id = 6000;",
                keys[1]
            ))
            .await
            .expect("write during the recompute");
        fx.d.release(&mut run, PausePoint::BeforeCorrectionCommit)
            .await;
        assert_eq!(run.finish().await.completed, 1);
        fx.drain(batch).await;
        fx.assert_right(&format!("{part:?} writes during the correction"))
            .await;
    }
}

/// A leaf detached `CONCURRENTLY`: its correction waits while the detach is
/// pending, and runs once it is finalized.
#[tokio::test]
async fn a_detach_pending_leaf_waits_for_the_detach_to_finalize() {
    for part in BOTH {
        let (t, n) = (part.table(), part.name());
        let mut fx = Fx::start(part, &tree(part, "range"), SEED).await;
        let holder = drain_driver::connect(fx.d.db.dsn()).await;
        holder
            .batch_execute(&format!(
                "begin; lock table public.{n}_p2 in access share mode"
            ))
            .await
            .expect("hold the leaf");
        let detacher = drain_driver::connect(fx.d.db.dsn()).await;
        detacher
            .batch_execute("set statement_timeout = '1500ms'")
            .await
            .expect("timeout");
        let interrupted = detacher
            .batch_execute(&format!(
                "alter table {t} detach partition public.{n}_p2 concurrently"
            ))
            .await;
        assert!(interrupted.is_err(), "the second phase waits on the holder");
        fx.pass().await;
        assert_eq!(fx.kinds().await, ["gone_node"]);
        let waiting = partition_correction::run_pending(fx.d.pool(), &Options::default())
            .await
            .expect("run");
        assert_eq!((waiting.completed, waiting.pages), (0, 0));
        holder.batch_execute("rollback").await.expect("release");
        fx.exec(&format!(
            "alter table {t} detach partition public.{n}_p2 finalize"
        ))
        .await;
        fx.correct().await;
        fx.assert_right(&format!("{part:?} detach finalized")).await;
        assert!(fx.queue().await.is_empty());
    }
}

// --- Two-level trees, DEFAULTs and HASH re-splits (#958) ------------------

/// `RANGE` then `HASH`: the root's ranges 0-7 and 8-15, the second a `HASH`
/// middle of four leaves.
fn two_level(part: Part) -> String {
    let (t, n) = (part.table(), part.name());
    format!(
        "{} create table public.{n}_lo partition of {t} for values from (0) to (8); \
         create table public.{n}_m partition of {t} for values from (8) to (16) \
             partition by hash (k); {}",
        head(part, "range"),
        (0..4)
            .map(|r| format!(
                "create table public.{n}_m{r} partition of public.{n}_m \
                 for values with (modulus 4, remainder {r});"
            ))
            .collect::<String>()
    )
}

/// Declares one `#[tokio::test]` per (operation, root) pair, each running
/// `$case(part, index)` on its own fixture. The cases used to be one test
/// looping over sixteen fixtures, a ~25 s tail no other test could overlap;
/// as separate tests the harness runs them in parallel (#1226).
macro_rules! case_tests {
    ($case:ident: $($index:literal => $fact:ident, $dim:ident;)+) => {
        $(
            #[tokio::test]
            async fn $fact() {
                $case(Part::Fact, $index).await;
            }

            #[tokio::test]
            async fn $dim() {
                $case(Part::Dim, $index).await;
            }
        )+
    };
}

/// One case's fixture: `schema`, the `op` run on it, then two correction
/// passes, the target compared with the oracle and the queue empty.
async fn run_lifecycle_case(part: Part, schema: &str, op: &str) {
    let what = format!("{part:?}: {op}");
    let mut fx = Fx::start(part, schema, SEED).await;
    fx.exec(op).await;
    fx.correct().await;
    fx.correct().await;
    fx.assert_right(&what).await;
    assert!(fx.queue().await.is_empty(), "{what}");
}

/// X12-7a: each lifecycle operation on a two-level tree, one per fixture.
async fn two_level_case(part: Part, index: usize) {
    let op = [
        "drop table public.{n}_m1",
        "alter table public.{n}_m detach partition public.{n}_m2",
        "truncate public.{n}_m3",
        "drop table public.{n}_m",
        "truncate public.{n}_m",
        "alter table {t} detach partition public.{n}_m; drop table public.{n}_m",
        "alter table public.{n}_m detach partition public.{n}_m0; \
         insert into public.{n}_m0 (id, k) select 7000, k from generate_series(8, 15) k \
             where satisfies_hash_partition('public.{n}_m'::regclass, 4, 0, k) limit 1; \
         alter table public.{n}_m attach partition public.{n}_m0 \
             for values with (modulus 4, remainder 0)",
        "drop table public.{n}_m1; \
         create table public.{n}_m1 partition of public.{n}_m \
             for values with (modulus 4, remainder 1); \
         insert into public.{n}_m1 (id, k) select 7100 + k, k from generate_series(8, 15) k \
             where satisfies_hash_partition('public.{n}_m'::regclass, 4, 1, k)",
    ][index];
    let op = op.replace("{n}", part.name()).replace("{t}", part.table());
    run_lifecycle_case(part, &two_level(part), &op).await;
}

case_tests! {
    two_level_case:
    0 => two_level_drop_a_leaf_fact, two_level_drop_a_leaf_dim;
    1 => two_level_detach_a_leaf_fact, two_level_detach_a_leaf_dim;
    2 => two_level_truncate_a_leaf_fact, two_level_truncate_a_leaf_dim;
    3 => two_level_drop_the_middle_node_fact, two_level_drop_the_middle_node_dim;
    4 => two_level_truncate_the_middle_node_fact, two_level_truncate_the_middle_node_dim;
    5 => two_level_detach_and_drop_the_middle_node_fact, two_level_detach_and_drop_the_middle_node_dim;
    6 => two_level_reattach_a_loaded_leaf_fact, two_level_reattach_a_loaded_leaf_dim;
    7 => two_level_recreate_a_leaf_loaded_fact, two_level_recreate_a_leaf_loaded_dim;
}

/// X12-7b: a `DEFAULT` as the node operated on: attached loaded, truncated,
/// detached, dropped; with siblings, and alone.
async fn default_case(part: Part, index: usize) {
    let (t, n) = (part.table(), part.name());
    let with_siblings = format!(
        "{} create table public.{n}_a partition of {t} for values from (0) to (8); \
         create table public.{n}_d partition of {t} default;",
        head(part, "range")
    );
    let alone = format!(
        "{} create table public.{n}_d partition of {t} default;",
        head(part, "list")
    );
    let (schema, op) = [
        (&with_siblings, "truncate public.{n}_d"),
        (&with_siblings, "drop table public.{n}_d"),
        (
            &with_siblings,
            "alter table {t} detach partition public.{n}_d",
        ),
        (
            &with_siblings,
            "alter table {t} detach partition public.{n}_d; \
             insert into public.{n}_d (id, k) values (8000, 30); \
             alter table {t} attach partition public.{n}_d default",
        ),
        (
            &with_siblings,
            "drop table public.{n}_a; insert into {t} (id, k) values (8100, 2)",
        ),
        (&alone, "truncate public.{n}_d"),
        (&alone, "drop table public.{n}_d"),
        (
            &alone,
            "alter table {t} detach partition public.{n}_d; \
             insert into public.{n}_d (id, k) values (8000, 30); \
             alter table {t} attach partition public.{n}_d default",
        ),
    ][index];
    let op = op.replace("{n}", n).replace("{t}", t);
    run_lifecycle_case(part, schema, &op).await;
}

case_tests! {
    default_case:
    0 => default_truncate_with_siblings_fact, default_truncate_with_siblings_dim;
    1 => default_drop_with_siblings_fact, default_drop_with_siblings_dim;
    2 => default_detach_with_siblings_fact, default_detach_with_siblings_dim;
    3 => default_reattach_loaded_with_siblings_fact, default_reattach_loaded_with_siblings_dim;
    4 => default_sibling_dropped_then_a_row_fact, default_sibling_dropped_then_a_row_dim;
    5 => default_truncate_alone_fact, default_truncate_alone_dim;
    6 => default_drop_alone_fact, default_drop_alone_dim;
    7 => default_reattach_loaded_alone_fact, default_reattach_loaded_alone_dim;
}

/// X12-7c: the `HASH` leaves of modulus 2 detached and re-attached, with new
/// tables, under modulus 4.
#[tokio::test]
async fn a_hash_resplit_converges() {
    for part in BOTH {
        let (t, n) = (part.table(), part.name());
        let mut fx = Fx::start(
            part,
            &format!(
                "{} create table public.{n}_a partition of {t} \
                     for values with (modulus 2, remainder 0); \
                 create table public.{n}_b partition of {t} \
                     for values with (modulus 2, remainder 1);",
                head(part, "hash")
            ),
            SEED,
        )
        .await;
        fx.exec(&format!(
            "begin; \
             alter table {t} detach partition public.{n}_b; \
             create table public.{n}_b1 (like {t} including all); \
             create table public.{n}_b3 (like {t} including all); \
             insert into public.{n}_b1 select * from public.{n}_b \
                 where satisfies_hash_partition('{t}'::regclass, 4, 1, k); \
             insert into public.{n}_b3 select * from public.{n}_b \
                 where satisfies_hash_partition('{t}'::regclass, 4, 3, k) and id % 2 = 0; \
             alter table {t} attach partition public.{n}_b1 \
                 for values with (modulus 4, remainder 1); \
             alter table {t} attach partition public.{n}_b3 \
                 for values with (modulus 4, remainder 3); \
             drop table public.{n}_b; commit;"
        ))
        .await;
        fx.correct().await;
        fx.correct().await;
        fx.assert_right(&format!("{part:?} hash re-split")).await;
        assert!(fx.queue().await.is_empty());
    }
}

// --- The complement sweep --------------------------------------------------

/// A leaf created, written through the parent and dropped between two
/// passes: no fingerprint sees it, and the sweep finds its cells in the
/// complement of the remaining leaves' bounds.
#[tokio::test]
async fn a_leaf_created_written_and_dropped_between_two_passes_converges() {
    for part in BOTH {
        for kind in ["hash", "range", "list"] {
            let what = format!("{part:?} {kind}");
            let (t, n) = (part.table(), part.name());
            let (leaves, missing) = match kind {
                "hash" => (
                    (0..3)
                        .map(|r| {
                            format!(
                                "create table public.{n}_p{r} partition of {t} \
                                 for values with (modulus 4, remainder {r});"
                            )
                        })
                        .collect::<String>(),
                    "for values with (modulus 4, remainder 3)",
                ),
                "range" => (
                    (0..3)
                        .map(|i| {
                            format!(
                                "create table public.{n}_p{i} partition of {t} \
                                 for values from ({}) to ({});",
                                i * 4,
                                i * 4 + 4
                            )
                        })
                        .collect::<String>(),
                    "for values from (12) to (16)",
                ),
                _ => (
                    (0..3)
                        .map(|i| {
                            format!(
                                "create table public.{n}_p{i} partition of {t} \
                                 for values in ({}, {}, {}, {});",
                                i * 4,
                                i * 4 + 1,
                                i * 4 + 2,
                                i * 4 + 3
                            )
                        })
                        .collect::<String>(),
                    "for values in (12, 13, 14, 15)",
                ),
            };
            // Whether key `k` is one the three leaves take.
            let route = |k: &str| {
                if kind == "hash" {
                    format!("not satisfies_hash_partition('{t}'::regclass, 4, 3, {k})")
                } else {
                    format!("{k} < 12")
                }
            };
            let seed = format!(
                "insert into public.b (id, k, gb, y) select * from \
                     (select i, i % 14, i % 3, i from generate_series(1, 28) i) s(id, k, gb, y) \
                     where {}; \
                 insert into public.a (id, ga, k, x) select * from \
                     (select i, i % 4, i % 16, i from generate_series(1, 160) i) s(id, ga, k, x) \
                     where {};",
                if part == Part::Dim {
                    route("k")
                } else {
                    "true".to_string()
                },
                if part == Part::Fact {
                    route("k")
                } else {
                    "true".to_string()
                },
            );
            let mut fx = Fx::start(part, &format!("{}{leaves}", head(part, kind)), &seed).await;
            fx.exec(&format!(
                "create table public.{n}_new partition of {t} {missing}"
            ))
            .await;
            fx.exec(&format!(
                "insert into {t} (id, k) select 1000 + i, i % 16 \
                     from generate_series(1, 160) i where not ({})",
                route("i % 16")
            ))
            .await;
            fx.d.settle().await;
            fx.exec(&format!("drop table public.{n}_new")).await;
            assert!(!fx.wrong_targets().await.is_empty(), "{what}");
            let outcome = fx.pass().await;
            assert!(outcome.partition_deep_checks.is_empty(), "{what}");
            assert_eq!(fx.kinds().await, ["complement"], "{what}");
            fx.run().await;
            fx.d.settle().await;
            fx.assert_right(&what).await;
            assert!(fx.queue().await.is_empty(), "{what}");
            fx.pass().await;
            assert!(
                fx.queue().await.is_empty(),
                "{what}: the next pass finds nothing"
            );
        }
    }
}

/// A recorded leaf without a usable text, dropped beside a `DEFAULT`: the
/// `DEFAULT`'s and the complement's corrections cover its cells.
#[tokio::test]
async fn a_gone_leaf_without_a_usable_text_beside_a_default_converges() {
    for part in BOTH {
        let (t, n) = (part.table(), part.name());
        let mut fx = Fx::start(part, &tree(part, "range"), SEED).await;
        fx.exec(&format!(
            "update partition_sources set nodes = jsonb_set(nodes, \
                 array['nodes', (select (i - 1)::text from jsonb_array_elements(nodes->'nodes') \
                                 with ordinality as e(n, i) \
                                 where n->>'name' = 'public.{n}_p1'), 'constraint'], \
                 'null'::jsonb)"
        ))
        .await;
        fx.exec(&format!(
            "drop table public.{n}_p1; create table public.{n}_d partition of {t} default;"
        ))
        .await;
        fx.converge_twice(&format!("{part:?} gone without a text"))
            .await;
    }
}

// --- Both sides partitioned ------------------------------------------------

/// Both tables partitioned on the join key, a leaf of each dropped and
/// another of each truncated in one interval: each side's corrections
/// recompute its own cells, in whichever order they run.
#[tokio::test]
async fn both_sides_partitioned_converge() {
    let leaves = |t: &str| {
        (0..4)
            .map(|r| {
                format!(
                    "create table public.{t}_p{r} partition of public.{t} \
                     for values with (modulus 4, remainder {r});"
                )
            })
            .collect::<String>()
    };
    let schema = format!(
        "create table public.a (id integer not null, {}, primary key (k, id)) \
             partition by hash (k); {} \
         create table public.b (id integer not null, {}, primary key (k, id)) \
             partition by hash (k); {}",
        columns(Part::Fact),
        leaves("a"),
        columns(Part::Dim),
        leaves("b"),
    );
    let mut fx = Fx::start(Part::Fact, &schema, SEED).await;
    fx.exec(
        "drop table public.a_p1; truncate public.a_p2; \
         drop table public.b_p2; truncate public.b_p1; \
         insert into public.b (id, k, gb, y) select 900 + i, i % 16, 5, 1 \
             from generate_series(1, 32) i where satisfies_hash_partition('public.b'::regclass, 4, 1, i % 16)",
    )
    .await;
    fx.correct().await;
    fx.correct().await;
    fx.assert_right("both sides").await;
    assert!(fx.queue().await.is_empty());
    assert!(
        partitions::pending_corrections(&fx.d.ctl, "public.b")
            .await
            .expect("b's queue")
            .is_empty()
    );
}
