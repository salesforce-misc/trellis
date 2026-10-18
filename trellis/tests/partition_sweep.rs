//! ADR-0017 P7 (issue #1066): every reconcile pass sweeps a partitioned
//! source for target groups that route to no current leaf, by the bounds'
//! complement, and queues their correction.
//!
//! Each test builds a partitioned `public.items`, an aggregate over it kept
//! live by the hand-driven pipeline (`support/drain_driver.rs`), and drives
//! the pass, the ring, the correction engine and the merger by hand. The
//! oracle is a from-scratch `GROUP BY` over the source. Nothing waits for
//! convergence (#297): the race test freezes the pass at a pause point.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::Driver;
use trellis::capture::complement::Sweep;
use trellis::capture::partitions::{self, PendingCorrection};
use trellis::capture::reconcile::PassOutcome;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::staging::interleave::PausePoint;
use trellis::staging::partition_correction::{self, Options};

const SOURCE: &str = "public.items";
const DEFINITION: &str =
    "TRANSFORM agg FROM public.items GROUP BY g SELECT SUM(v) AS total, COUNT(*) AS n";

struct Fx {
    d: Driver,
}

impl Fx {
    async fn start(ddl: &str, key: ValueType) -> Fx {
        trellis::defs::partition_tree::open_gate_for_tests();
        let d = Driver::start(
            ddl,
            &[
                ("id", ValueType::Numeric),
                ("g", key),
                ("v", ValueType::Numeric),
            ],
            &[DEFINITION],
            &[SOURCE],
        )
        .await;
        let mut fx = Fx { d };
        fx.pass().await;
        assert!(fx.queue().await.is_empty());
        assert!(fx.wrong_groups().await.is_empty());
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

    /// The pass's sweep of the source.
    async fn sweep(&mut self) -> Sweep {
        sweep_of(&self.pass().await)
    }

    /// The definition's status.
    async fn status(&self) -> String {
        self.d
            .ctl
            .query_one(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = 'agg'",
                &[],
            )
            .await
            .expect("status")
            .get(0)
    }

    async fn queue(&self) -> Vec<PendingCorrection> {
        partitions::pending_corrections(&self.d.ctl, SOURCE)
            .await
            .expect("the queue")
    }

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
                 order by 1",
            )
            .await
    }

    /// Drains the ring, runs the engine to its end and merges its deltas.
    async fn correct(&mut self) {
        self.d.settle().await;
        partition_correction::run_pending(self.d.pool(), &Options::default())
            .await
            .expect("run the pending corrections");
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

fn sweep_of(outcome: &PassOutcome) -> Sweep {
    outcome
        .partition_sweeps
        .iter()
        .find(|(t, _)| t == SOURCE)
        .map(|(_, s)| s.clone())
        .expect("the source was swept")
}

fn kinds(queue: &[PendingCorrection]) -> Vec<&str> {
    queue.iter().map(|c| c.kind.as_str()).collect()
}

/// `items` partitioned `kind` on `g` with leaves for the groups 0-11 only,
/// ten rows a group; and the bound of a leaf for the groups 12-15.
fn tree(kind: &str) -> (String, &'static str) {
    let head = format!(
        "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by {kind} (g);"
    );
    let (leaves, rows, missing) = match kind {
        // Remainder 3 of 4 is missing. The rows are the groups 0-15 whose
        // hash isn't, and the missing leaf takes the others.
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
             where not satisfies_hash_partition('public.items'::regclass, 4, 3, i % 16);",
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
            "insert into public.items select i, i % 12, i from generate_series(1, 120) i;",
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
            "insert into public.items select i, i % 12, i from generate_series(1, 120) i;",
            "for values in (12, 13, 14, 15)",
        ),
    };
    (format!("{head}{leaves}{rows}"), missing)
}

/// Rows through the parent for every group 0-15 the missing leaf takes.
const WRITE_MISSING: &str = "insert into public.items select 1000 + i, i % 16, 7 \
     from generate_series(1, 160) i where (i % 16) not in (select g from public.items)";

#[tokio::test]
async fn a_leaf_created_written_and_dropped_between_two_passes_converges() {
    for kind in ["hash", "range", "list"] {
        let (ddl, missing) = tree(kind);
        let mut fx = Fx::start(&ddl, ValueType::Numeric).await;
        // Nothing routes nowhere yet.
        let quiet = fx.sweep().await;
        assert!(!quiet.queued, "{kind}: {quiet:?}");
        assert_eq!(quiet.statements, 1, "{kind}: the complement is searched");

        // Between two passes: a leaf, rows through the parent (staged by the
        // root's capture), applied, and the leaf dropped. Neither
        // fingerprint changes.
        fx.exec(&format!(
            "create table public.items_new partition of public.items {missing}"
        ))
        .await;
        fx.exec(WRITE_MISSING).await;
        fx.d.settle().await;
        fx.exec("drop table public.items_new").await;
        let counted = fx.wrong_groups().await;
        assert!(!counted.is_empty(), "{kind}: the dropped rows are counted");

        let outcome = fx.pass().await;
        assert!(
            outcome.partition_deep_checks.is_empty(),
            "{kind}: no fingerprint saw it"
        );
        let sweep = sweep_of(&outcome);
        assert!(sweep.queued, "{kind}: {sweep:?}");
        assert_eq!(kinds(&fx.queue().await), ["complement"], "{kind}");
        // Scheduled as any correction is: a `correction` row that holds the
        // definition `backfilling` until it drains.
        partition_correction::schedule(fx.d.pool())
            .await
            .expect("schedule the corrections");
        assert_eq!(fx.status().await, "backfilling", "{kind}");
        fx.correct().await;
        assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{kind}");
        assert!(fx.queue().await.is_empty(), "{kind}");
        trellis::staging::build::settle_builds(fx.d.pool()).await;
        assert_eq!(
            fx.status().await,
            "live",
            "{kind}: the drained correction releases the definition"
        );
        // And the next pass finds nothing.
        assert!(!fx.sweep().await.queued, "{kind}");
        assert!(fx.queue().await.is_empty(), "{kind}");
    }
}

#[tokio::test]
async fn no_sweep_statement_runs_beside_a_default_or_with_every_hash_remainder() {
    for ddl in [
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_a partition of public.items for values from (0) to (8); \
         create table public.items_d partition of public.items default; \
         insert into public.items select i, i % 16, i from generate_series(1, 160) i;",
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by list (g); \
         create table public.items_d partition of public.items default; \
         insert into public.items select i, i % 16, i from generate_series(1, 160) i;",
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by hash (g); \
         create table public.items_a partition of public.items \
             for values with (modulus 2, remainder 0); \
         create table public.items_b partition of public.items \
             for values with (modulus 4, remainder 1); \
         create table public.items_c partition of public.items \
             for values with (modulus 4, remainder 3); \
         insert into public.items select i, i % 16, i from generate_series(1, 160) i;",
    ] {
        let mut fx = Fx::start(ddl, ValueType::Numeric).await;
        let sweep = fx.sweep().await;
        assert_eq!(
            (
                sweep.statements,
                sweep.queued,
                sweep.attempts,
                sweep.skipped
            ),
            (0, false, 1, false),
            "{ddl}"
        );
    }
}

/// A gone leaf's queued correction covers its groups: the sweep doesn't
/// queue them again, and the queue drains to a converged target.
#[tokio::test]
async fn a_dropped_recorded_leaf_is_left_to_its_own_correction() {
    for kind in ["hash", "range", "list"] {
        let (ddl, _) = tree(kind);
        let mut fx = Fx::start(&ddl, ValueType::Numeric).await;
        fx.exec("drop table public.items_p1").await;
        let sweep = fx.sweep().await;
        assert!(!sweep.queued, "{kind}: {sweep:?}");
        assert_eq!(kinds(&fx.queue().await), ["gone_node"], "{kind}");
        fx.correct().await;
        assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{kind}");
    }
}

/// A recorded node whose text is unusable (detached between the tree read
/// and the text fetch, here by blanking the recorded text) and then dropped:
/// its groups route to no leaf, or to the `DEFAULT` or new leaf that took
/// its bound, and are corrected through the complement or that node's own
/// correction.
#[tokio::test]
async fn a_gone_node_without_a_usable_text_converges() {
    for replacement in [
        "",
        "create table public.items_d partition of public.items default;",
        "create table public.items_again partition of public.items for values from (4) to (8);",
    ] {
        let (ddl, _) = tree("range");
        let mut fx = Fx::start(&ddl, ValueType::Numeric).await;
        fx.exec(
            "update partition_sources set nodes = jsonb_set(nodes, \
                 array['nodes', (select (i - 1)::text from jsonb_array_elements(nodes->'nodes') \
                                 with ordinality as e(n, i) \
                                 where n->>'name' = 'public.items_p1'), 'constraint'], \
                 'null'::jsonb)",
        )
        .await;
        fx.exec("drop table public.items_p1").await;
        fx.exec(replacement).await;
        fx.pass().await;
        let queue = fx.queue().await;
        assert!(
            queue
                .iter()
                .any(|c| c.kind == "gone_node" && c.constraint.is_none()),
            "{replacement}: {:?}",
            kinds(&queue)
        );
        fx.correct().await;
        // The engine's walk leaves the next pass to find what's left.
        fx.pass().await;
        fx.correct().await;
        assert_eq!(
            fx.wrong_groups().await,
            Vec::<String>::new(),
            "{replacement}"
        );
        assert!(fx.queue().await.is_empty(), "{replacement}");
    }
}

/// The race the order of the reads closes: a leaf created after the sweep
/// read the bounds, whose rows the stale complement holds, is seen by the
/// second fingerprint read, and the attempt that found them rolls back.
#[tokio::test]
async fn a_leaf_created_between_the_catalog_read_and_the_search_is_not_corrected() {
    let (ddl, missing) = tree("range");
    let mut fx = Fx::start(&ddl, ValueType::Numeric).await;
    let dsn = fx.d.db.dsn().to_string();
    let mut pass =
        fx.d.run_frozen(
            &[(PausePoint::AfterSweepCatalogRead, SOURCE)],
            move |pool| async move {
                let mut client = drain_driver::connect(&dsn).await;
                let desired = trellis::defs::tables_to_capture(&pool)
                    .await
                    .expect("tables to capture");
                Ok(trellis::capture::reconcile::reconcile(
                    &mut client,
                    DEFAULT_SCHEMA,
                    &desired,
                    Instant::now() + Duration::from_secs(5),
                )
                .await
                .expect("pass"))
            },
        )
        .await;
    pass.reached(PausePoint::AfterSweepCatalogRead).await;
    // The sweep has read the bounds without the new leaf. Create it, write
    // its groups through the parent and apply them.
    fx.exec(&format!(
        "create table public.items_new partition of public.items {missing}"
    ))
    .await;
    fx.exec(WRITE_MISSING).await;
    fx.d.settle().await;
    assert!(
        fx.d.rows("select g::text from public.agg where g >= 12")
            .await
            .len()
            == 4,
        "the new leaf's groups are in the target"
    );
    fx.d.release(&mut pass, PausePoint::AfterSweepCatalogRead)
        .await;
    let outcome = pass.finish().await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    let sweep = sweep_of(&outcome);
    assert_eq!(sweep.attempts, 2, "{sweep:?}");
    assert!(!sweep.queued, "{sweep:?}");
    assert!(
        !kinds(&fx.queue().await).contains(&"complement"),
        "the new leaf's rows are not queued for correction"
    );
    // The next pass records the leaf (a correction of its own rows, which
    // re-derives them as they are), and the target stays right.
    fx.pass().await;
    fx.correct().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
    assert!(
        fx.d.rows("select g::text from public.agg where g >= 12")
            .await
            .len()
            == 4
    );
}

/// `RANGE` bounds are ordered by the key column's collation, not the
/// database's: under `"C"` every upper case letter sorts before every lower
/// case one, under the database's ICU `en-US` they interleave.
#[tokio::test]
async fn range_gaps_are_ordered_by_the_key_columns_collation() {
    let mut fx = Fx::start(
        "create table public.items (id integer not null, g text collate \"C\" not null, \
             v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_up partition of public.items for values from ('A') to ('B'); \
         create table public.items_low partition of public.items for values from ('a') to ('b'); \
         insert into public.items values (1, 'A1', 1), (2, 'A2', 2), (3, 'a1', 3);",
        ValueType::Text,
    )
    .await;
    // Under `en-US`, 'A1' would fall in a gap before 'a'.
    let sweep = fx.sweep().await;
    assert!(!sweep.queued, "{sweep:?}");
    // 'M1' sorts between 'B' and 'a' only under "C".
    fx.exec(
        "create table public.items_mid partition of public.items for values from ('B') to ('a'); \
         insert into public.items values (4, 'M1', 4);",
    )
    .await;
    fx.d.settle().await;
    fx.exec("drop table public.items_mid").await;
    let sweep = fx.sweep().await;
    assert!(sweep.queued, "{sweep:?}");
    fx.correct().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
}

/// `--ignored`: what the sweep costs a pass at 1,000 leaves (ADR-0017
/// reports the figures): the catalog reads and the search, for `HASH` with
/// every remainder present and with two missing, `RANGE` and `LIST`; and,
/// for comparison, the full routing form (every recorded leaf text over every
/// target group) the sweep doesn't run. Each prints `MEASURE` lines.
async fn measure(kind: &str, ddl: &str) -> Fx {
    let started = Instant::now();
    // The group column typed as define gives it from the source (`integer`),
    // so the search's cast to the key's type is a no-op an index serves.
    let mut fx = Fx::start(ddl, ValueType::Integer(trellis::IntWidth::Int4)).await;
    let groups: i64 =
        fx.d.ctl
            .query_one("select count(*) from public.agg", &[])
            .await
            .expect("count")
            .get(0);
    eprintln!(
        "MEASURE kind={kind} setup_s={:.1} target_rows={groups}",
        started.elapsed().as_secs_f64()
    );
    // Before and after the target has statistics: a fresh build's target
    // has none until autovacuum analyzes it, and the search's plan for a
    // `RANGE` gap is an index scan only once it has.
    for analyzed in [false, true] {
        if analyzed {
            fx.exec("analyze public.agg").await;
        }
        for _ in 0..5 {
            let sweep = fx.sweep().await;
            eprintln!(
                "MEASURE kind={kind} analyzed={analyzed} statements={} read_ms={:.2} \
                 search_ms={:.2}",
                sweep.statements,
                sweep.read.as_secs_f64() * 1e3,
                sweep.search.as_secs_f64() * 1e3,
            );
        }
    }
    fx
}

async fn measure_full_routing_form(fx: &Fx, kind: &str) {
    let recorded = partitions::recorded(&fx.d.ctl, SOURCE)
        .await
        .expect("recorded")
        .expect("a recorded tree");
    let texts: Vec<String> = recorded
        .nodes
        .iter()
        .skip(1)
        .filter_map(|n| n.constraint.clone())
        .collect();
    let sql = format!(
        "select count(*) from (select (t.g)::integer as g from public.agg t) r \
         where not ({})",
        texts.join(" or ")
    );
    for _ in 0..3 {
        let started = Instant::now();
        let txn_sql = format!("begin; {}; {sql}; commit;", partitions::pinned_settings());
        fx.exec(&txn_sql).await;
        eprintln!(
            "MEASURE kind={kind} full_routing_form_s={:.2} texts={}",
            started.elapsed().as_secs_f64(),
            texts.len()
        );
    }
}

fn thousand_leaves(kind: &str) -> String {
    let leaves: String = (0..1000)
        .map(|i| match kind {
            "hash" => format!(
                "create table public.items_p{i} partition of public.items \
                 for values with (modulus 1000, remainder {i});"
            ),
            "range" => format!(
                "create table public.items_p{i} partition of public.items \
                 for values from ({}) to ({});",
                i * 1000,
                i * 1000 + 1000
            ),
            _ => format!(
                "create table public.items_p{i} partition of public.items \
                 for values in ({});",
                (0..10)
                    .map(|j| (i * 10 + j).to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        })
        .collect();
    let rows = match kind {
        // 1M rows over 1M groups; for `HASH`, without remainders 998 and 999.
        "hash" => {
            "insert into public.items select i, i, 1 from generate_series(0, 999999) i \
                   where not satisfies_hash_partition('public.items'::regclass, 1000, 998, i) \
                     and not satisfies_hash_partition('public.items'::regclass, 1000, 999, i);"
        }
        "range" => "insert into public.items select i, i, 1 from generate_series(0, 999999) i;",
        // 1M rows over the 10,000 listed values.
        _ => "insert into public.items select i, i % 10000, 1 from generate_series(0, 999999) i;",
    };
    let leaves = if kind == "hash" {
        // Leave out remainders 998 and 999 for now.
        leaves
            .split_inclusive(';')
            .filter(|l| !l.contains("remainder 998)") && !l.contains("remainder 999)"))
            .collect()
    } else {
        leaves
    };
    format!(
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by {kind} (g); \
         {leaves} {rows} analyze public.items;"
    )
}

#[tokio::test]
#[ignore = "a measurement: run with logbook reserve and a memory cap"]
async fn measure_the_sweep_at_a_thousand_hash_leaves() {
    let mut fx = measure("hash_two_missing", &thousand_leaves("hash")).await;
    measure_full_routing_form(&fx, "hash").await;
    fx.exec(
        "create table public.items_p998 partition of public.items \
             for values with (modulus 1000, remainder 998); \
         create table public.items_p999 partition of public.items \
             for values with (modulus 1000, remainder 999);",
    )
    .await;
    fx.pass().await;
    for _ in 0..5 {
        let sweep = fx.sweep().await;
        eprintln!(
            "MEASURE kind=hash_none_missing statements={} read_ms={:.2} search_ms={:.2}",
            sweep.statements,
            sweep.read.as_secs_f64() * 1e3,
            sweep.search.as_secs_f64() * 1e3,
        );
    }
}

#[tokio::test]
#[ignore = "a measurement: run with logbook reserve and a memory cap"]
async fn measure_the_sweep_at_a_thousand_range_leaves() {
    let fx = measure("range", &thousand_leaves("range")).await;
    measure_full_routing_form(&fx, "range").await;
}

#[tokio::test]
#[ignore = "a measurement: run with logbook reserve and a memory cap"]
async fn measure_the_sweep_at_a_thousand_list_leaves() {
    let fx = measure("list", &thousand_leaves("list")).await;
    measure_full_routing_form(&fx, "list").await;
}

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

/// Two passes, each followed by the engine: the second finds what the first
/// pass's corrections left. The target was wrong before and is right after.
async fn converge_twice(fx: &mut Fx, what: &str) {
    assert!(
        !fx.wrong_groups().await.is_empty(),
        "{what}: rows to correct"
    );
    fx.pass().await;
    fx.correct().await;
    fx.pass().await;
    fx.correct().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{what}");
    assert!(fx.queue().await.is_empty(), "{what}");
}

/// The cases below each take a gone node without a usable text, or a
/// short-lived node, through a shape the sweep or a `DEFAULT`'s own
/// correction must cover, with no full routing form.
///
/// A `HASH` modulus change: a recorded `(2, 1)` leaf without a usable text is
/// dropped and replaced by `(4, 1)`, and a `(4, 3)` leaf is created, written
/// and dropped in the same interval, so its remainder routes nowhere.
#[tokio::test]
async fn a_hash_modulus_change_around_a_gone_node_without_a_usable_text_converges() {
    let mut fx = Fx::start(
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by hash (g); \
         create table public.items_a partition of public.items \
             for values with (modulus 2, remainder 0); \
         create table public.items_b partition of public.items \
             for values with (modulus 2, remainder 1); \
         insert into public.items select i, i % 16, i from generate_series(1, 160) i;",
        ValueType::Numeric,
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
    converge_twice(&mut fx, "hash modulus change").await;
}

/// A middle whose children are partly gone: one child without a usable text
/// dropped, and a sibling leaf created, written and dropped.
#[tokio::test]
async fn a_middle_whose_children_are_partly_gone_converges() {
    let mut fx = Fx::start(
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_lo partition of public.items for values from (0) to (4); \
         create table public.items_m partition of public.items for values from (4) to (12) \
             partition by list (g); \
         create table public.items_m1 partition of public.items_m for values in (4, 5, 6, 7); \
         create table public.items_m2 partition of public.items_m for values in (8, 9); \
         insert into public.items select i, i % 10, i from generate_series(1, 100) i;",
        ValueType::Numeric,
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
    converge_twice(&mut fx, "middle partly gone").await;
}

/// Two operations in one interval beside a `DEFAULT`: a recorded leaf without
/// a usable text dropped, and a leaf created over part of its range, written
/// and dropped. The `DEFAULT`'s siblings changed, so its own correction walks
/// the whole range.
#[tokio::test]
async fn two_operations_in_one_pass_beside_a_default_converge() {
    let (ddl, _) = tree("range");
    let mut fx = Fx::start(
        &format!("{ddl} create table public.items_d partition of public.items default;"),
        ValueType::Numeric,
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
    converge_twice(&mut fx, "default beside two operations").await;
}

/// A `DEFAULT` created in the interval a leaf is created, written and
/// dropped: the new `DEFAULT`'s own correction walks its range.
#[tokio::test]
async fn a_default_created_as_a_short_lived_leaf_is_dropped_converges() {
    let (ddl, missing) = tree("range");
    let mut fx = Fx::start(&ddl, ValueType::Numeric).await;
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
    converge_twice(&mut fx, "default created with the leaf gone").await;
}

/// A `DEFAULT` created, written and dropped between two passes: its range is
/// the complement.
#[tokio::test]
async fn a_default_created_written_and_dropped_between_two_passes_converges() {
    let (ddl, _) = tree("list");
    let mut fx = Fx::start(&ddl, ValueType::Numeric).await;
    fx.exec(
        "create table public.items_d partition of public.items default; \
         insert into public.items select 5000 + i, 20 + i % 5, 1 from generate_series(1, 50) i;",
    )
    .await;
    fx.d.settle().await;
    fx.exec("drop table public.items_d").await;
    converge_twice(&mut fx, "default created and dropped").await;
}

/// A leaf attached with rows, written through the parent, detached and
/// dropped between two passes.
#[tokio::test]
async fn a_leaf_attached_written_and_dropped_between_two_passes_converges() {
    let (ddl, _) = tree("range");
    let mut fx = Fx::start(&ddl, ValueType::Numeric).await;
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
    converge_twice(&mut fx, "attached and dropped").await;
}

/// A complement that names a `HASH` middle, queued while the middle is in
/// the tree, runs after the middle is dropped: it can't be evaluated, so it
/// completes without work, and the middle's own correction, queued by the
/// next pass, covers its groups.
#[tokio::test]
async fn a_complement_whose_hash_middle_is_dropped_completes_and_converges() {
    let mut fx = Fx::start(
        "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_lo partition of public.items for values from (0) to (4); \
         create table public.items_m partition of public.items for values from (4) to (20) \
             partition by hash (g); \
         create table public.items_m0 partition of public.items_m \
             for values with (modulus 4, remainder 0); \
         create table public.items_m1 partition of public.items_m \
             for values with (modulus 4, remainder 1); \
         create table public.items_m2 partition of public.items_m \
             for values with (modulus 4, remainder 2); \
         insert into public.items select i, i % 20, i from generate_series(1, 200) i \
             where i % 20 < 4 \
                or not satisfies_hash_partition('public.items_m'::regclass, 4, 3, i % 20);",
        ValueType::Numeric,
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
    let sweep = fx.sweep().await;
    assert!(sweep.queued, "{sweep:?}");
    let queue = fx.queue().await;
    assert_eq!(kinds(&queue), ["complement"]);
    let text = queue[0].constraint.clone().expect("a complement text");
    assert!(text.contains("satisfies_hash_partition"), "{text}");
    fx.exec("drop table public.items_m").await;
    fx.correct().await;
    assert!(
        fx.queue().await.is_empty(),
        "the complement completes once its HASH middle is gone"
    );
    // The middle's rows went with it; its own correction clears its groups.
    assert!(!fx.wrong_groups().await.is_empty());
    fx.pass().await;
    assert_eq!(kinds(&fx.queue().await), ["gone_node"]);
    fx.correct().await;
    assert_eq!(fx.wrong_groups().await, Vec::<String>::new());
    assert!(fx.queue().await.is_empty());
}

// ---------------------------------------------------------------------------
// #1129: real ways to reach a recorded node that is gone with an unusable
// text, and the shapes that could let an unchanged leaf absorb its region.
// Each drives real DDL against the real pass, freezing it between the tree
// read and the text fetch where the window needs widening.
// ---------------------------------------------------------------------------

impl Fx {
    /// A pass run in its own task, frozen at `point` for the source.
    async fn frozen_pass(&self, point: PausePoint) -> drain_driver::Running<PassOutcome> {
        let dsn = self.d.db.dsn().to_string();
        self.d
            .run_frozen(&[(point, SOURCE)], move |pool| async move {
                let mut client = drain_driver::connect(&dsn).await;
                let desired = trellis::defs::tables_to_capture(&pool)
                    .await
                    .expect("tables to capture");
                Ok(trellis::capture::reconcile::reconcile(
                    &mut client,
                    DEFAULT_SCHEMA,
                    &desired,
                    Instant::now() + Duration::from_secs(5),
                )
                .await
                .expect("pass"))
            })
            .await
    }

    /// The recorded node `name`: `None` when it isn't recorded, else its
    /// recorded text.
    async fn recorded_text(&self, name: &str) -> Option<Option<String>> {
        partitions::recorded(&self.d.ctl, SOURCE)
            .await
            .expect("recorded")
            .and_then(|r| r.nodes.into_iter().find(|n| n.name == name))
            .map(|n| n.constraint)
    }

    /// Runs `ddl` while a pass is frozen between its tree read and its text
    /// fetch, then lets the pass finish.
    async fn pass_around_the_fetch(&mut self, ddl: &str) -> PassOutcome {
        let mut pass = self.frozen_pass(PausePoint::BeforeConstraintFetch).await;
        pass.reached(PausePoint::BeforeConstraintFetch).await;
        self.exec(ddl).await;
        self.d
            .release(&mut pass, PausePoint::BeforeConstraintFetch)
            .await;
        let outcome = pass.finish().await;
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
        outcome
    }
}

/// Passes, each followed by the engine, until the target matches the oracle
/// and nothing is queued, with no full routing form. Returns how many passes
/// it took and every correction queued meanwhile (`kind node cause`), which
/// says which of the three states the region ended in.
async fn converge(fx: &mut Fx, what: &str, max: usize) -> (usize, Vec<String>) {
    let mut queued: Vec<String> = Vec::new();
    for n in 1..=max {
        fx.pass().await;
        for c in fx.queue().await {
            let line = format!("{} {} {}", c.kind, c.node_name, c.cause);
            if !queued.contains(&line) {
                queued.push(line);
            }
        }
        fx.correct().await;
        if fx.wrong_groups().await.is_empty() && fx.queue().await.is_empty() {
            eprintln!("CONVERGED {what}: passes={n} queued={queued:?}");
            return (n, queued);
        }
    }
    panic!(
        "{what}: not converged after {max} passes: {:?}, queued {queued:?}",
        fx.wrong_groups().await
    );
}

/// Rows through the parent for the groups 12-15, which the leaf for them
/// takes.
const WRITE_THROUGH_X: &str = "insert into public.items select 7000 + i, 12 + i % 4, 2 \
     from generate_series(1, 8) i";

/// The region of a leaf for the groups 12-15 after it is gone, by what took
/// it: nothing, the `DEFAULT`, a new leaf of the same bound, or two new
/// leaves that split it.
const REPLACEMENTS: [(&str, &str); 4] = [
    ("uncovered", ""),
    (
        "default",
        "create table public.items_d partition of public.items default;",
    ),
    (
        "new leaf",
        "create table public.items_again partition of public.items for values from (12) to (16);",
    ),
    (
        "split",
        "create table public.items_s1 partition of public.items for values from (12) to (14); \
         create table public.items_s2 partition of public.items for values from (14) to (16);",
    ),
];

/// A leaf attached with rows of its own and written through the parent, read
/// by a pass, detached before the pass fetches its text, then dropped: a
/// recorded node gone with no usable text. The drop and what takes the
/// region come either in the same window, so the pass's own sweep sees the
/// region taken, or before the next pass, with or without the engine
/// running between.
#[tokio::test]
async fn a_leaf_detached_between_the_tree_read_and_the_text_fetch_converges() {
    for timing in ["in the window", "before the next pass", "engine between"] {
        for (state, replacement) in REPLACEMENTS {
            let what = format!("{state}, {timing}");
            let (ddl, missing) = tree("range");
            let mut fx = Fx::start(&ddl, ValueType::Numeric).await;
            fx.exec(&format!(
                "create table public.items_x (like public.items including all); \
                 insert into public.items_x select 9000 + i, 12 + i % 4, 1 \
                     from generate_series(1, 8) i; \
                 alter table public.items attach partition public.items_x {missing};"
            ))
            .await;
            fx.exec(WRITE_THROUGH_X).await;
            fx.d.settle().await;
            let gone = format!("drop table public.items_x; {replacement}");
            let window = if timing == "in the window" {
                format!("alter table public.items detach partition public.items_x; {gone}")
            } else {
                "alter table public.items detach partition public.items_x".to_string()
            };
            fx.pass_around_the_fetch(&window).await;
            assert_eq!(
                fx.recorded_text("public.items_x").await,
                Some(None),
                "{what}: recorded without a usable text"
            );
            if timing == "engine between" {
                fx.correct().await;
            }
            if timing != "in the window" {
                fx.exec(&gone).await;
            }
            if state != "uncovered" {
                // Rows the new owner of the region takes, applied normally.
                fx.exec(
                    "insert into public.items select 8000 + i, 12 + i % 4, 5 \
                     from generate_series(1, 8) i",
                )
                .await;
                fx.d.settle().await;
            }
            if timing != "engine between" {
                assert!(
                    !fx.wrong_groups().await.is_empty(),
                    "{what}: rows to correct"
                );
            }
            let (_, queued) = converge(&mut fx, &what, 4).await;
            assert!(
                queued
                    .iter()
                    .any(|q| q.starts_with("gone_node public.items_x")),
                "{what}: {queued:?}"
            );
        }
    }
}

/// Attaches `public.items_x` for the groups 12-15 with rows of its own and
/// writes the region through the parent, applied, so the next pass reads the
/// leaf as created and fetches its text.
async fn attach_x(fx: &mut Fx, missing: &str) {
    fx.exec(&format!(
        "create table public.items_x (like public.items including all); \
         insert into public.items_x select 9000 + i, 12 + i % 4, 1 \
             from generate_series(1, 8) i; \
         alter table public.items attach partition public.items_x {missing};"
    ))
    .await;
    fx.exec(WRITE_THROUGH_X).await;
    fx.d.settle().await;
}

/// A `DETACH ... CONCURRENTLY` of `public.items_x` held in its second
/// transaction by an open reader of the parent. `cancel` cancels it there,
/// which leaves the detach pending (`inhdetachpending`); otherwise
/// [`Self::finish`] lets it complete.
struct ConcurrentDetach {
    holder: tokio_postgres::Client,
    task: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    pid: i32,
}

impl ConcurrentDetach {
    async fn start(fx: &Fx) -> ConcurrentDetach {
        let dsn = fx.d.db.dsn().to_string();
        let holder = drain_driver::connect(&dsn).await;
        holder
            .batch_execute("begin; select count(*) from public.items")
            .await
            .expect("hold the parent");
        let holder_pid: i32 = holder
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        let detacher = drain_driver::connect(&dsn).await;
        let pid: i32 = detacher
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        let task = tokio::spawn(async move {
            detacher
                .batch_execute(
                    "alter table public.items detach partition public.items_x concurrently",
                )
                .await
        });
        fx.d.wait_blocked_behind(holder_pid).await;
        assert!(fx.detach_pending().await, "the first transaction committed");
        ConcurrentDetach { holder, task, pid }
    }

    /// Cancels the detach in its wait, leaving it pending.
    async fn cancel(self, fx: &Fx) {
        fx.exec(&format!("select pg_cancel_backend({})", self.pid))
            .await;
        let result = self.task.await.expect("task");
        assert!(result.is_err(), "the detach was cancelled");
        self.holder
            .batch_execute("rollback")
            .await
            .expect("rollback");
        assert!(fx.detach_pending().await, "left pending");
    }

    /// Lets the detach complete.
    async fn finish(self) {
        self.holder
            .batch_execute("rollback")
            .await
            .expect("rollback");
        self.task
            .await
            .expect("task")
            .expect("the detach completes");
    }
}

impl Fx {
    async fn detach_pending(&self) -> bool {
        self.d
            .ctl
            .query_one(
                "select coalesce((select inhdetachpending from pg_inherits \
                                  where inhrelid = 'public.items_x'::regclass), false)",
                &[],
            )
            .await
            .expect("inhdetachpending")
            .get(0)
    }
}

/// `DETACH ... CONCURRENTLY` around the text fetch: started (its first
/// transaction committed) between the tree read and the fetch and then left
/// pending; left pending and finalized in that window; or completing after
/// the fetch. A pending node's text is still fetched; a finalized one reads
/// none. Each converges once the detach is final, which a `gone_node`
/// correction waits for.
#[tokio::test]
async fn a_concurrent_detach_around_the_text_fetch_converges() {
    for case in [
        "left pending",
        "finalized in the window",
        "completes after the fetch",
    ] {
        let (ddl, missing) = tree("range");
        let mut fx = Fx::start(&ddl, ValueType::Numeric).await;
        attach_x(&mut fx, missing).await;
        let mut pass = fx.frozen_pass(PausePoint::BeforeConstraintFetch).await;
        pass.reached(PausePoint::BeforeConstraintFetch).await;
        let detach = ConcurrentDetach::start(&fx).await;
        let running = match case {
            "left pending" => {
                detach.cancel(&fx).await;
                None
            }
            "finalized in the window" => {
                detach.cancel(&fx).await;
                fx.exec("alter table public.items detach partition public.items_x finalize")
                    .await;
                None
            }
            _ => Some(detach),
        };
        fx.d.release(&mut pass, PausePoint::BeforeConstraintFetch)
            .await;
        let outcome = pass.finish().await;
        assert!(outcome.failed.is_empty(), "{case}: {:?}", outcome.failed);
        // A detach-pending leaf still has its text; a finalized one has none.
        let text = fx.recorded_text("public.items_x").await;
        assert_eq!(
            text.as_ref().map(Option::is_some),
            Some(case != "finalized in the window"),
            "{case}: {text:?}"
        );
        if let Some(detach) = running {
            detach.finish().await;
        }
        if case == "left pending" {
            // The next pass reads the pending detach as gone; its correction
            // waits for the detach to be final.
            fx.pass().await;
            assert!(
                fx.queue()
                    .await
                    .iter()
                    .any(|c| c.kind == "gone_node" && c.node_name == "public.items_x"),
                "{case}"
            );
            fx.correct().await;
            assert!(!fx.queue().await.is_empty(), "{case}: waits while pending");
            // A read of the parent still sees the detach-pending leaf's rows,
            // so nothing is wrong while the correction waits.
            assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{case}");
            fx.exec("alter table public.items detach partition public.items_x finalize")
                .await;
        }
        fx.exec("drop table public.items_x").await;
        let (_, queued) = converge(&mut fx, case, 4).await;
        assert!(
            queued
                .iter()
                .any(|q| q.starts_with("gone_node public.items_x")),
            "{case}: {queued:?}"
        );
    }
}

/// One window case: `tree` with an aggregate kept live; `attach` (a node the
/// next pass reads as new, with rows `write` puts through the parent,
/// applied); a pass that runs `window` between its tree read and its text
/// fetch, after which `node` is recorded without a usable text; `after`
/// before the next pass, then rows `owner_write` puts through the parent
/// into whatever owns the region now, applied. Returns the passes to
/// converge and the corrections queued.
struct Window<'a> {
    what: &'a str,
    tree: &'a str,
    attach: &'a str,
    write: &'a str,
    node: &'a str,
    window: &'a str,
    after: &'a str,
    owner_write: &'a str,
}

impl Window<'_> {
    async fn run(&self) -> (usize, Vec<String>) {
        let mut fx = Fx::start(self.tree, ValueType::Numeric).await;
        fx.exec(self.attach).await;
        fx.exec(self.write).await;
        fx.d.settle().await;
        fx.pass_around_the_fetch(self.window).await;
        assert_eq!(
            fx.recorded_text(self.node).await,
            Some(None),
            "{}: recorded without a usable text",
            self.what
        );
        fx.exec(self.after).await;
        fx.exec(self.owner_write).await;
        fx.d.settle().await;
        assert!(
            !fx.wrong_groups().await.is_empty(),
            "{}: rows to correct",
            self.what
        );
        let (passes, queued) = converge(&mut fx, self.what, 4).await;
        assert!(
            queued
                .iter()
                .any(|q| q.starts_with(&format!("gone_node {}", self.node))),
            "{}: {queued:?}",
            self.what
        );
        (passes, queued)
    }
}

/// Mixed `HASH` moduli: a `(4, 3)` leaf beside `(2, 0)` and `(4, 1)`, read
/// and detached in the window and dropped; its remainder is left uncovered,
/// taken by a new `(4, 3)`, or split into `(8, 3)` and `(8, 7)`. Postgres
/// refuses a bound that overlaps, so neither unchanged leaf can take it.
#[tokio::test]
async fn mixed_hash_moduli_around_a_node_without_a_usable_text_converge() {
    let tree = "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by hash (g); \
         create table public.items_a partition of public.items \
             for values with (modulus 2, remainder 0); \
         create table public.items_b partition of public.items \
             for values with (modulus 4, remainder 1); \
         insert into public.items select i, i % 32, i from generate_series(1, 320) i \
             where not satisfies_hash_partition('public.items'::regclass, 4, 3, i % 32);";
    let into_x = "insert into public.items select 7000 + i, i % 32, 2 from generate_series(1, 320) i \
             where satisfies_hash_partition('public.items'::regclass, 4, 3, i % 32)";
    let into_owner = "insert into public.items select 8000 + i, i % 32, 5 from generate_series(1, 320) i \
             where satisfies_hash_partition('public.items'::regclass, 4, 3, i % 32)";
    for (state, after, owner_write) in [
        ("uncovered", "", ""),
        (
            "new leaf",
            "create table public.items_again partition of public.items \
                 for values with (modulus 4, remainder 3);",
            into_owner,
        ),
        (
            "split",
            "create table public.items_s1 partition of public.items \
                 for values with (modulus 8, remainder 3); \
             create table public.items_s2 partition of public.items \
                 for values with (modulus 8, remainder 7);",
            into_owner,
        ),
    ] {
        Window {
            what: &format!("hash {state}"),
            tree,
            attach: "create table public.items_x partition of public.items \
                         for values with (modulus 4, remainder 3);",
            write: into_x,
            node: "public.items_x",
            window: "alter table public.items detach partition public.items_x; \
                     drop table public.items_x;",
            after,
            owner_write,
        }
        .run()
        .await;
    }
}

/// A `DEFAULT` that is itself partitioned takes the region of a leaf read
/// and detached in the window: its children cover all of it, or only part,
/// leaving the rest in the `DEFAULT`'s own gap.
#[tokio::test]
async fn a_partitioned_default_taking_a_node_without_a_usable_text_converges() {
    let (base, missing) = tree("range");
    for (state, children) in [
        (
            "partitioned default",
            "create table public.items_d1 partition of public.items_d for values in (12, 13); \
             create table public.items_d2 partition of public.items_d for values in (14, 15, 16);",
        ),
        (
            "partitioned default, part in its gap",
            "create table public.items_d1 partition of public.items_d for values in (12, 13);",
        ),
    ] {
        Window {
            what: state,
            tree: &format!(
                "{base} create table public.items_d partition of public.items default \
                     partition by list (g); {children}"
            ),
            attach: &format!("create table public.items_x partition of public.items {missing};"),
            write: WRITE_THROUGH_X,
            node: "public.items_x",
            window: "alter table public.items detach partition public.items_x; \
                     drop table public.items_x;",
            after: "",
            owner_write: "insert into public.items select 8000 + i, 12 + i % 2, 5 \
                          from generate_series(1, 8) i",
        }
        .run()
        .await;
    }
}

/// A two-level tree with a `DEFAULT` under the middle, or under the root
/// beside a middle without one: a leaf under the middle, read and detached
/// in the window, dropped. Its region goes to the middle's `DEFAULT`, or is
/// left in the middle's own gap, which the root's `DEFAULT` doesn't take.
#[tokio::test]
async fn a_default_on_a_two_level_tree_around_a_node_without_a_usable_text_converges() {
    let head = "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_lo partition of public.items for values from (0) to (4); \
         create table public.items_m partition of public.items for values from (4) to (16) \
             partition by list (g); \
         create table public.items_m1 partition of public.items_m for values in (4, 5, 6, 7); \
         insert into public.items select i, i % 8, i from generate_series(1, 80) i;";
    for (state, extra, owner_write) in [
        (
            "default middle",
            "create table public.items_md partition of public.items_m default;",
            "insert into public.items select 8000 + i, 12 + i % 4, 5 from generate_series(1, 8) i",
        ),
        (
            "root default, middle gap",
            "create table public.items_d partition of public.items default;",
            "insert into public.items select 8000 + i, 20 + i % 4, 5 from generate_series(1, 8) i",
        ),
    ] {
        Window {
            what: state,
            tree: &format!("{head} {extra}"),
            attach: "create table public.items_x partition of public.items_m \
                         for values in (12, 13, 14, 15);",
            write: WRITE_THROUGH_X,
            node: "public.items_x",
            window: "alter table public.items_m detach partition public.items_x; \
                     drop table public.items_x;",
            after: "",
            owner_write,
        }
        .run()
        .await;
    }
}

/// A leaf read and detached in the window, then, before the next pass,
/// changed while detached and attached again under the same bound in one
/// transaction; or attached again under the same bound in the window itself.
#[tokio::test]
async fn a_node_without_a_usable_text_attached_again_under_its_bound_converges() {
    let (base, missing) = tree("range");
    let reattach = format!(
        "begin; \
         delete from public.items_x where id % 2 = 0; \
         insert into public.items_x select 9500 + i, 12 + i % 4, 3 from generate_series(1, 8) i; \
         alter table public.items attach partition public.items_x {missing}; \
         commit;"
    );
    for (state, window, after) in [
        (
            "re-attached before the next pass",
            "alter table public.items detach partition public.items_x".to_string(),
            reattach.clone(),
        ),
        (
            "re-attached in the window",
            format!("alter table public.items detach partition public.items_x; {reattach}"),
            String::new(),
        ),
    ] {
        let mut fx = Fx::start(&base, ValueType::Numeric).await;
        attach_x(&mut fx, missing).await;
        fx.pass_around_the_fetch(&window).await;
        // Attached again before the fetch, the leaf has its text again.
        let text = fx.recorded_text("public.items_x").await;
        assert_eq!(
            text.as_ref().map(Option::is_some),
            Some(state == "re-attached in the window"),
            "{state}: {text:?}"
        );
        fx.exec(&after).await;
        let (_, queued) = converge(&mut fx, state, 4).await;
        assert!(
            queued
                .iter()
                .any(|q| q.starts_with("new_leaf public.items_x")),
            "{state}: {queued:?}"
        );
    }
}

/// A recorded leaf detached, changed and attached again under the same bound
/// in one transaction between two passes: its `pg_inherits.xmin` moved, so
/// it reads as re-attached.
#[tokio::test]
async fn a_leaf_detached_and_attached_again_in_one_transaction_converges() {
    let (ddl, _) = tree("list");
    let mut fx = Fx::start(&ddl, ValueType::Numeric).await;
    fx.exec(
        "begin; \
         alter table public.items detach partition public.items_p1; \
         delete from public.items_p1 where id % 3 = 0; \
         insert into public.items_p1 select 9000 + i, 4 + i % 4, 1 from generate_series(1, 8) i; \
         alter table public.items attach partition public.items_p1 for values in (4, 5, 6, 7); \
         commit;",
    )
    .await;
    let (_, queued) = converge(&mut fx, "detach and attach in one transaction", 4).await;
    assert_eq!(queued, ["new_leaf public.items_p1 reattached"]);
}

/// Several operations in one pass interval around a node without a usable
/// text: in the window, the new leaf is detached and dropped, a recorded
/// leaf is dropped, a leaf is created over half of the first one's range
/// and a `DEFAULT` takes the rest; before the next pass, rows go through the
/// parent and the half leaf is dropped too.
#[tokio::test]
async fn several_operations_around_a_node_without_a_usable_text_converge() {
    let (base, missing) = tree("range");
    Window {
        what: "several operations",
        tree: &base,
        attach: &format!("create table public.items_x partition of public.items {missing};"),
        write: WRITE_THROUGH_X,
        node: "public.items_x",
        window: "alter table public.items detach partition public.items_x; \
                 drop table public.items_x; \
                 drop table public.items_p1; \
                 create table public.items_h partition of public.items for values from (12) to (14); \
                 create table public.items_d partition of public.items default;",
        after: "insert into public.items select 8000 + i, 4 + i % 12, 5 from generate_series(1, 24) i; \
                drop table public.items_h;",
        owner_write: "",
    }
    .run()
    .await;
}

/// A `HASH` middle dropped while a correction naming one of its leaves is
/// queued and not yet run: a dropped leaf's `gone_node`, a new leaf's
/// `new_leaf` (whose text embeds the middle's oid), or a leaf read and
/// detached in the window (no usable text).
#[tokio::test]
async fn a_hash_middle_dropped_under_a_queued_correction_converges() {
    let tree = "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_lo partition of public.items for values from (0) to (4); \
         create table public.items_m partition of public.items for values from (4) to (20) \
             partition by hash (g); \
         create table public.items_m0 partition of public.items_m \
             for values with (modulus 4, remainder 0); \
         create table public.items_m1 partition of public.items_m \
             for values with (modulus 4, remainder 1); \
         create table public.items_m2 partition of public.items_m \
             for values with (modulus 4, remainder 2); \
         insert into public.items select i, i % 20, i from generate_series(1, 200) i \
             where i % 20 < 4 \
                or not satisfies_hash_partition('public.items_m'::regclass, 4, 3, i % 20);";
    let into_m3 = "insert into public.items select 5000 + i, i % 20, 7 from generate_series(1, 200) i \
             where i % 20 >= 4 \
               and satisfies_hash_partition('public.items_m'::regclass, 4, 3, i % 20);";
    let new_m3 = "create table public.items_m3 partition of public.items_m \
             for values with (modulus 4, remainder 3);";
    for case in ["gone_node", "new_leaf", "no usable text"] {
        let mut fx = Fx::start(tree, ValueType::Numeric).await;
        fx.exec(new_m3).await;
        fx.exec(into_m3).await;
        fx.d.settle().await;
        match case {
            "gone_node" => {
                fx.pass().await;
                fx.correct().await;
                fx.exec("drop table public.items_m1").await;
                fx.pass().await;
                assert_eq!(kinds(&fx.queue().await), ["gone_node"], "{case}");
            }
            "new_leaf" => {
                fx.pass().await;
                assert_eq!(kinds(&fx.queue().await), ["new_leaf"], "{case}");
            }
            _ => {
                fx.pass_around_the_fetch(
                    "alter table public.items_m detach partition public.items_m3",
                )
                .await;
                assert_eq!(fx.recorded_text("public.items_m3").await, Some(None));
            }
        }
        // The middle goes before the engine takes the queued correction.
        fx.exec("drop table public.items_m").await;
        if case == "no usable text" {
            fx.exec("drop table public.items_m3").await;
        }
        fx.d.settle().await;
        // A new leaf's text embeds the dropped middle's oid; the leaf went
        // with its middle, so its correction is complete without work
        // (#1148), and the middle's `gone_node` corrects the region.
        partition_correction::run_pending(fx.d.pool(), &Options::default())
            .await
            .expect("run the pending corrections");
        let plan = fx.d.build_plan("agg").await;
        fx.d.merge_all(&plan).await;
        let (_, queued) = converge(&mut fx, case, 4).await;
        assert!(
            queued
                .iter()
                .any(|q| q.starts_with("gone_node public.items_m ")),
            "{case}: {queued:?}"
        );
    }
}

/// A middle dropped and created again under the same name between two
/// passes, with new rows; and a middle read in the window, detached there,
/// dropped and created again under the same name.
#[tokio::test]
async fn a_middle_dropped_and_created_again_under_its_name_converges() {
    let head = "create table public.items (id integer not null, g integer not null, \
             v integer not null, primary key (g, id)) partition by range (g); \
         create table public.items_lo partition of public.items for values from (0) to (4);";
    let middle = "create table public.items_m partition of public.items for values from (4) to (12) \
             partition by list (g); \
         create table public.items_m1 partition of public.items_m for values in (4, 5, 6, 7); \
         create table public.items_m2 partition of public.items_m for values in (8, 9, 10, 11);";
    let rows = "insert into public.items select 100 + i, i % 12, i from generate_series(1, 120) i;";
    let recreate = "create table public.items_m partition of public.items for values from (4) to (12) \
             partition by list (g); \
         create table public.items_m1 partition of public.items_m for values in (4, 5); \
         insert into public.items select 6000 + i, 4 + i % 2, 9 from generate_series(1, 10) i;";

    // Recorded with its text, dropped and created again between two passes.
    let mut fx = Fx::start(&format!("{head} {middle} {rows}"), ValueType::Numeric).await;
    fx.exec(&format!("drop table public.items_m; {recreate}"))
        .await;
    let (_, queued) = converge(&mut fx, "middle re-created", 4).await;
    assert!(
        queued
            .iter()
            .any(|q| q.starts_with("gone_node public.items_m ")),
        "{queued:?}"
    );

    // Read in the window as a new middle, detached there: no usable text.
    Window {
        what: "middle without a usable text, re-created",
        tree: &format!(
            "{head} insert into public.items select i, i % 4, i from generate_series(1, 40) i;"
        ),
        attach: middle,
        write: rows,
        node: "public.items_m",
        window: "alter table public.items detach partition public.items_m; \
                 drop table public.items_m;",
        after: recreate,
        owner_write: "",
    }
    .run()
    .await;
}

/// A plain `DETACH` committed while the pass's text fetch waits on its lock:
/// the fetch's `pg_get_partition_constraintdef` finds the node no longer a
/// partition once it has the lock.
#[tokio::test]
async fn a_detach_committed_during_the_text_fetch_converges() {
    for ddl in [
        "alter table public.items detach partition public.items_x",
        "drop table public.items_x",
    ] {
        let (base, missing) = tree("range");
        let mut fx = Fx::start(&base, ValueType::Numeric).await;
        attach_x(&mut fx, missing).await;
        let mut pass = fx.frozen_pass(PausePoint::BeforeConstraintFetch).await;
        pass.reached(PausePoint::BeforeConstraintFetch).await;
        let holder = drain_driver::connect(fx.d.db.dsn()).await;
        holder
            .batch_execute(&format!("begin; {ddl}"))
            .await
            .expect("hold the DDL");
        let holder_pid: i32 = holder
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        fx.d.release(&mut pass, PausePoint::BeforeConstraintFetch)
            .await;
        fx.d.wait_blocked_behind(holder_pid).await;
        holder.batch_execute("commit").await.expect("commit");
        // The fetch takes the node's lock once the DDL commits and finds it
        // no longer a partition (`XX000`): it defers the check and records
        // nothing (#1149). Past the fetch's lock timeout it retries and reads
        // no text; past its last retry it defers too (a slow runner can
        // commit that late). Each way the pass fails nothing, and the next
        // pass reads the tree afresh.
        let outcome = pass.finish().await;
        let recorded = fx.recorded_text("public.items_x").await;
        assert!(
            outcome.failed.is_empty() && matches!(recorded, Some(None) | None),
            "{ddl}: {:?} {recorded:?}",
            outcome.failed
        );
        if ddl.starts_with("alter") {
            fx.exec("drop table public.items_x").await;
        }
        converge(&mut fx, ddl, 4).await;
    }
}

/// A range tree with a `HASH` middle for the groups 4-19 that has no leaf
/// for remainder 3, ten rows a group outside that remainder.
const HASH_MIDDLE_TREE: &str = "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by range (g); \
     create table public.items_lo partition of public.items for values from (0) to (4); \
     create table public.items_m partition of public.items for values from (4) to (20) \
         partition by hash (g); \
     create table public.items_m0 partition of public.items_m \
         for values with (modulus 4, remainder 0); \
     create table public.items_m1 partition of public.items_m \
         for values with (modulus 4, remainder 1); \
     create table public.items_m2 partition of public.items_m \
         for values with (modulus 4, remainder 2); \
     insert into public.items select i, i % 20, i from generate_series(1, 200) i \
         where i % 20 < 4 \
            or not satisfies_hash_partition('public.items_m'::regclass, 4, 3, i % 20);";

/// The middle's remainder-3 leaf, with rows through the parent.
const HASH_MIDDLE_M3: &str = "create table public.items_m3 partition of public.items_m \
         for values with (modulus 4, remainder 3); \
     insert into public.items select 5000 + i, i % 20, 7 from generate_series(1, 200) i \
         where i % 20 >= 4 \
           and satisfies_hash_partition('public.items_m'::regclass, 4, 3, i % 20);";

/// Rows through the parent for the groups 4-19, into whatever owns them.
const INTO_THE_MIDDLES_REGION: &str =
    "insert into public.items select 8000 + i, 4 + i % 16, 5 from generate_series(1, 64) i;";

/// A `HASH` leaf read and detached in the window (no usable text) and
/// dropped; then, in one interval, its middle is dropped and a root
/// `DEFAULT` takes the middle's whole bound. The engine runs before the next
/// pass.
#[tokio::test]
async fn a_hash_middle_dropped_and_a_default_created_around_a_node_without_a_usable_text_converge()
{
    let mut fx = Fx::start(HASH_MIDDLE_TREE, ValueType::Numeric).await;
    fx.exec(HASH_MIDDLE_M3).await;
    fx.d.settle().await;
    fx.pass_around_the_fetch(
        "alter table public.items_m detach partition public.items_m3; \
         drop table public.items_m3",
    )
    .await;
    assert_eq!(fx.recorded_text("public.items_m3").await, Some(None));
    fx.exec(&format!(
        "drop table public.items_m; \
         create table public.items_d partition of public.items default; \
         {INTO_THE_MIDDLES_REGION}"
    ))
    .await;
    fx.d.settle().await;
    assert!(!fx.wrong_groups().await.is_empty(), "rows to correct");
    fx.correct().await;
    let (_, queued) = converge(&mut fx, "middle dropped, default created", 4).await;
    assert!(
        queued
            .iter()
            .any(|q| q == "new_leaf public.items_d created")
            && queued
                .iter()
                .any(|q| q.starts_with("gone_node public.items_m ")),
        "{queued:?}"
    );
}

/// A `HASH` middle detached, not dropped, while a new leaf's `new_leaf`
/// (whose text names the middle) is queued: the engine runs before the next
/// pass and can still evaluate the text. The detached middle is dropped
/// later.
#[tokio::test]
async fn a_hash_middle_detached_under_a_queued_new_leaf_converges() {
    let mut fx = Fx::start(HASH_MIDDLE_TREE, ValueType::Numeric).await;
    fx.exec(HASH_MIDDLE_M3).await;
    fx.d.settle().await;
    fx.pass().await;
    assert_eq!(kinds(&fx.queue().await), ["new_leaf"]);
    fx.exec("alter table public.items detach partition public.items_m")
        .await;
    fx.d.settle().await;
    fx.correct().await;
    let (_, queued) = converge(&mut fx, "middle detached", 4).await;
    assert_eq!(queued, ["gone_node public.items_m gone"]);
    fx.exec("drop table public.items_m").await;
    converge(&mut fx, "detached middle dropped", 4).await;
}

/// A new `HASH` leaf read by a pass whose window detaches its middle and
/// attaches a middle of another modulus under the same bound; the old
/// middle is dropped before the next pass, with rows for the new one.
#[tokio::test]
async fn a_hash_middle_replaced_by_another_modulus_around_the_fetch_converges() {
    let mut fx = Fx::start(HASH_MIDDLE_TREE, ValueType::Numeric).await;
    fx.exec(HASH_MIDDLE_M3).await;
    fx.d.settle().await;
    fx.pass_around_the_fetch(
        "alter table public.items detach partition public.items_m; \
         create table public.items_n partition of public.items for values from (4) to (20) \
             partition by hash (g); \
         create table public.items_n0 partition of public.items_n \
             for values with (modulus 2, remainder 0); \
         create table public.items_n1 partition of public.items_n \
             for values with (modulus 2, remainder 1);",
    )
    .await;
    fx.exec(&format!(
        "drop table public.items_m; {INTO_THE_MIDDLES_REGION}"
    ))
    .await;
    fx.d.settle().await;
    assert!(!fx.wrong_groups().await.is_empty(), "rows to correct");
    let (_, queued) = converge(&mut fx, "middle replaced", 4).await;
    assert!(
        queued
            .iter()
            .any(|q| q == "new_leaf public.items_n0 created")
            && queued
                .iter()
                .any(|q| q.starts_with("gone_node public.items_m ")),
        "{queued:?}"
    );
}
