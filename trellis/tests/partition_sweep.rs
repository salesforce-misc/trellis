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
        partition_correction::run_pending(self.d.pool(), SOURCE, &Options::default())
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
        fx.correct().await;
        assert_eq!(fx.wrong_groups().await, Vec::<String>::new(), "{kind}");
        assert!(fx.queue().await.is_empty(), "{kind}");
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
