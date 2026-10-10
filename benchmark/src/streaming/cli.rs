//! Argument parsing and dispatch for the streaming scenarios.
//!
//! Kept out of `main.rs` so the backfill scenarios there stay readable: these
//! scenarios carry far more knobs than the `--n`/`--g`/`--ceiling-secs`
//! shape `main.rs`'s own parser handles, and they all share the same
//! [`EngineTuning`] flags.

use std::time::Duration;

use crate::streaming::rate::{human_rate, restaged_in_window};
use crate::streaming::tuning::EngineTuning;
use crate::streaming::write_tax::{self, CellOptions, ProbeMode, Shape, Variant};
use crate::streaming::{
    build_under_load, capture_ceiling, disk_tier, fold_in, generator_reach, hop_latency, idle_cost,
    load, ssi_tax, throughput,
};

/// Every scenario name this module handles, for `main.rs`'s usage message.
pub const SCENARIOS: &[&str] = &[
    "hop-ladder",
    "hop-latency",
    "throughput-ramp",
    "transaction-shape",
    "fold-in-ratio",
    "group-contention",
    "write-tax",
    "capture-ceiling",
    "ssi-tax",
    "idle-cost",
    "generator-reach",
    "build-under-load",
];

/// The latency ladder's defaults. #266: "low offered rate (e.g. 10
/// commits/s)". 60 s at 10 commits/sec gives ~600 end-to-end observations per
/// depth — enough for the p99 bucket-fraction check to mean something (a single
/// miss out of 600 is already inside the 1% tolerance).
const LADDER_DEFAULT_RATE: f64 = 10.0;
const LADDER_DEFAULT_DURATION: Duration = Duration::from_secs(60);

/// The ramp's default candidates (rows/sec), spanning well below and above
/// T2's 100k target so the knee is bracketed rather than guessed at.
const RAMP_DEFAULT_RATES: &[f64] = &[
    1_000.0, 5_000.0, 10_000.0, 25_000.0, 50_000.0, 100_000.0, 200_000.0,
];
const RAMP_DEFAULT_DURATION: Duration = Duration::from_secs(20);
const RAMP_DEFAULT_GRACE: Duration = Duration::from_secs(30);

/// The transaction-shape sweep's default target rate: comfortably under the
/// measured single-hop knee, so a `kept_target_rate: false` there means the *shape*
/// hurt rather than that the rate alone had already saturated the pipeline
/// regardless of shape.
const SHAPE_DEFAULT_TARGET_RATE: f64 = 20_000.0;
const SHAPE_DEFAULT_DURATION: Duration = Duration::from_secs(20);
const SHAPE_DEFAULT_GRACE: Duration = Duration::from_secs(30);

/// The fold-in sweep's defaults: #266's own ratio list at T3's 400k rows/sec.
const FOLD_IN_DEFAULT_TARGET_RATE: f64 = 400_000.0;
const FOLD_IN_DEFAULT_DURATION: Duration = Duration::from_secs(20);
/// Longer than every other scenario's 30s grace (issue #270 review): at
/// 400k rows/sec every ratio starts the grace window already backlogged, and a low fold-in ratio's target
/// write count is close to the row count — near the same write load as a 1-1
/// chain, whose own knee sits at ~210-230k rows/sec. Measured directly: ratio
/// 1000:1 needs on the order of 90-120s to fully drain and pass its oracle;
/// 30s reports every ratio as `drained: false` with `oracle_ok: null`
/// (unevaluated, not failed — see `check_aggregate_oracle`), which is not a
/// real reproduction of a T3 measurement. A low ratio (e.g. 10:1, 40,000
/// groups at this target rate) may still not drain even at 120s — that is
/// this scenario's own hypothesis under test (throughput should scale with
/// fold-in ratio), not a harness fault, and is why this raises the grace
/// rather than lowering the default target rate.
const FOLD_IN_DEFAULT_GRACE: Duration = Duration::from_secs(120);

/// Issue #277's group-count axis: the five points it names, bracketing
/// #268's 1000:1 (400 groups at 400k rows/sec) by two decades either side.
const CONTENTION_DEFAULT_GROUPS: &[usize] = &[10, 100, 400, 4_000, 40_000];

/// Repetitions per cell for `write-tax` and `capture-ceiling`: the plan asks
/// for at least three, interleaved (#622 C4).
const WRITE_TAX_DEFAULT_REPS: usize = 3;

/// Idle cost's defaults: 8 drain threads (#269's own "staging worker + 8 drain
/// threads" wording), a 10 s warmup so start-of-day work never pollutes the
/// steady-state sample, then a 60 s window — long enough that a few hundred
/// transactions/sec is measured over tens of thousands of transactions rather
/// than a handful.
const IDLE_DEFAULT_WARMUP: Duration = Duration::from_secs(10);
const IDLE_DEFAULT_DURATION: Duration = Duration::from_secs(60);

const REACH_DEFAULT_ROWS_PER_COMMIT: usize = 1000;
const REACH_DEFAULT_DURATION: Duration = Duration::from_secs(10);

/// `build-under-load`'s knobs (see [`build_under_load`]'s module doc for the
/// scenario), over #617's defaults:
///
/// - `--rows <n>` (10,000,000) COPY-loaded into `agg_src`, `grp` uniform over
///   `--groups <n>` (100,000), by `--loaders <n>` (4) connections;
/// - `--writers <n>` (8; 0 for a build-only run) paced writers at
///   `--write-rate <stmt/s>` (2,000 in total), starting `--pre-define-secs`
///   (2) before the definition and running `--duration-secs` (20) after it
///   reads `live`;
/// - `--build-timeout-secs` (3,600) for define -> `live`, `--grace-secs`
///   (600) for the target to converge once the writers stop, and at least
///   `--oracle-poll-min-secs` (5) between full-source oracle comparisons after
///   a mismatch;
/// - `--progress-secs` (30; 0 turns it off): how often stderr gets an RSS
///   progress line;
/// - `--min-max`: the definition also has `MIN(amt)` and `MAX(amt)`,
///   recomputed fields (#625 F5);
/// - `--one-to-one`: the definition is a 1-1 target, `SELECT grp AS grp,
///   amt + amt AS dbl`, instead of the aggregate (#625 F8a);
/// - `--apply-latency` (with `--one-to-one`): once it is `live`, time every
///   statement `apply` accepts while the writers run, and each field
///   rebuild to `live` (`apply_latency`, #666, #625 F8b);
///   `--apply-latency-big-to-side` adds a relationship whose to-side is the
///   loaded source.
///
/// Plus the engine flags every throughput scenario takes (`--application-threads`,
/// 8 by default; `--poll-interval-ms`, `--maintenance-interval-ms`,
/// `--reconcile-interval-ms`, `--drain-batch-cap`), and #625 F2's
/// `--build-chunk-rows <n>` (the Re-derive build's chunk size, 10,000). Postgres settings for a disk
/// run go through testkit's `TRELLIS_TESTKIT_PG_OPTIONS`, e.g.
/// `'shared_buffers=1GB checkpoint_timeout=1min max_wal_size=4GB'` (#617's).
fn build_under_load_config(args: &[String]) -> build_under_load::BuildUnderLoad {
    let positive = |name: &str, default: f64| {
        let v = number(args, name).unwrap_or(default);
        assert!(v > 0.0, "{name} must be positive, got {v}");
        v
    };
    let progress = number(args, "--progress-secs").unwrap_or(30.0);
    assert!(
        progress >= 0.0,
        "--progress-secs must not be negative, got {progress}"
    );
    build_under_load::BuildUnderLoad {
        rows: positive("--rows", 10_000_000.0) as u64,
        groups: positive("--groups", 100_000.0) as i32,
        loaders: positive("--loaders", 4.0) as usize,
        writers: {
            let v = number(args, "--writers").unwrap_or(8.0);
            assert!(v >= 0.0, "--writers must not be negative, got {v}");
            v as usize
        },
        write_rate: positive("--write-rate", 2_000.0),
        pre_define: secs(args, "--pre-define-secs").unwrap_or(Duration::from_secs(2)),
        post_live: secs(args, "--duration-secs").unwrap_or(Duration::from_secs(20)),
        build_timeout: secs(args, "--build-timeout-secs").unwrap_or(Duration::from_secs(3600)),
        grace: secs(args, "--grace-secs").unwrap_or(Duration::from_secs(600)),
        oracle_poll_min: secs(args, "--oracle-poll-min-secs").unwrap_or(Duration::from_secs(5)),
        progress: (progress > 0.0).then(|| Duration::from_secs_f64(progress)),
        min_max: args.iter().any(|a| a == "--min-max"),
        one_to_one: args.iter().any(|a| a == "--one-to-one"),
        apply_latency: args.iter().any(|a| a == "--apply-latency"),
        apply_big_to_side: args.iter().any(|a| a == "--apply-latency-big-to-side"),
    }
}

/// `--connections <n>`: the multi-connection generator's
/// ([`load::run_parallel_load`]) writer count, for every scenario that uses
/// it — everything except the latency ladder, which stays on the
/// single-connection paced generator by design. Absent, the caller falls back
/// to [`load::DEFAULT_CONNECTIONS`].
fn connections(args: &[String]) -> Option<usize> {
    number(args, "--connections").map(|n| {
        assert!(n >= 1.0, "--connections must be at least 1, got {n}");
        n as usize
    })
}

/// The `--connections`/`--duration-secs`/`--grace-secs` trio every
/// throughput probe shares, over the scenario's own defaults.
fn offer(args: &[String], duration: Duration, grace: Duration) -> throughput::Offer {
    throughput::Offer {
        connections: connections(args).unwrap_or(load::DEFAULT_CONNECTIONS),
        duration: secs(args, "--duration-secs").unwrap_or(duration),
        grace: secs(args, "--grace-secs").unwrap_or(grace),
    }
}

fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn number(args: &[String], name: &str) -> Option<f64> {
    flag(args, name).map(|raw| {
        raw.trim()
            .parse()
            .unwrap_or_else(|e| panic!("{name} value {raw:?} must be a number: {e}"))
    })
}

fn millis(args: &[String], name: &str) -> Option<Duration> {
    number(args, name).map(|ms| Duration::from_millis(ms as u64))
}

fn secs(args: &[String], name: &str) -> Option<Duration> {
    number(args, name).map(Duration::from_secs_f64)
}

/// Parses a comma-separated numeric list (`--rates 1000,5000,10000`), or
/// `None` when the flag is absent. Panics on a malformed element rather than
/// silently dropping it.
fn number_list(args: &[String], name: &str) -> Option<Vec<f64>> {
    flag(args, name).map(|raw| {
        raw.split(',')
            .map(|s| {
                s.trim()
                    .parse()
                    .unwrap_or_else(|e| panic!("{name} value {s:?} must be a number: {e}"))
            })
            .collect()
    })
}

fn usize_list(args: &[String], name: &str, default: &[usize]) -> Vec<usize> {
    match number_list(args, name) {
        Some(values) => values.into_iter().map(|v| v as usize).collect(),
        None => default.to_vec(),
    }
}

/// The [`EngineTuning`] flags every streaming scenario accepts, over
/// `default` (which differs per scenario — the latency ladder and the
/// throughput scenarios want different drain-worker counts).
fn tuning(args: &[String], default: EngineTuning) -> EngineTuning {
    EngineTuning {
        staging_worker: default.staging_worker && !args.iter().any(|a| a == "--no-staging-worker"),
        application_threads: number(args, "--application-threads")
            .map(|v| v as usize)
            .unwrap_or(default.application_threads),
        poll_interval: millis(args, "--poll-interval-ms").unwrap_or(default.poll_interval),
        maintenance_interval: millis(args, "--maintenance-interval-ms")
            .unwrap_or(default.maintenance_interval),
        reconcile_interval: millis(args, "--reconcile-interval-ms")
            .unwrap_or(default.reconcile_interval),
        drain_batch_cap: number(args, "--drain-batch-cap")
            .map(|v| v as usize)
            .unwrap_or(default.drain_batch_cap),
        build_chunk_rows: number(args, "--build-chunk-rows")
            .map(|v| {
                assert!(v >= 1.0, "--build-chunk-rows must be at least 1, got {v}");
                v as i64
            })
            .unwrap_or(default.build_chunk_rows),
    }
}

fn throughput_tuning(args: &[String]) -> EngineTuning {
    tuning(
        args,
        EngineTuning {
            application_threads: throughput::THROUGHPUT_APPLICATION_THREADS,
            ..Default::default()
        },
    )
}

/// `--variants a,b,…` by name ([`Variant::parse`]), or `default`.
fn variants(args: &[String], default: &[Variant]) -> Vec<Variant> {
    match flag(args, "--variants") {
        Some(raw) => raw.split(',').map(Variant::parse).collect(),
        None => default.to_vec(),
    }
}

fn reps(args: &[String]) -> usize {
    let reps = number(args, "--reps")
        .map(|v| v as usize)
        .unwrap_or(WRITE_TAX_DEFAULT_REPS);
    assert!(reps >= 1, "--reps must be at least 1");
    reps
}

/// The flags every `write-tax`/`capture-ceiling` cell shares, over
/// [`CellOptions::default`]: `--max-secs`, `--rows`, `--copy-rows` and
/// `--snapshot-probe off|orm|all`.
fn cell_options(args: &[String]) -> CellOptions {
    let default = CellOptions::default();
    CellOptions {
        max_window: secs(args, "--max-secs").unwrap_or(default.max_window),
        copy_rows: number(args, "--copy-rows")
            .map(|v| v as u64)
            .unwrap_or(default.copy_rows),
        rows: number(args, "--rows").map(|v| v as u64).or(default.rows),
        probe: flag(args, "--snapshot-probe")
            .map(ProbeMode::parse)
            .unwrap_or(default.probe),
    }
}

/// The runtime `write-tax` and `capture-ceiling` run their writers on, its
/// threads named `bench-writer`.
fn writer_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("bench-writer")
        .build()
        .expect("build the writer runtime")
}

/// Runs `name` if it is one of [`SCENARIOS`], returning `Some(true)` when
/// every result it printed looked trustworthy and `Some(false)` when a hard
/// failure (an oracle mismatch, or a metric cross-check that failed) means the
/// numbers shouldn't be believed. `None` when `name` isn't a streaming
/// scenario, so `main.rs` can carry on to the backfill ones.
///
/// Note what does **not** make this return `false`: missing a T1 target, or a
/// rate failing to sustain. Those are the measurements, and #269 is explicit
/// that a red number is a finding to report rather than something to tune
/// until it passes. Only a harness- or correctness-level failure is an error.
pub fn run(name: &str, args: &[String]) -> Option<bool> {
    let runtime = || tokio::runtime::Runtime::new().expect("build tokio runtime");

    match name {
        "hop-ladder" | "hop-latency" => {
            let depths = if name == "hop-latency" {
                vec![number(args, "--depth").expect("hop-latency requires --depth <hops>") as usize]
            } else {
                usize_list(args, "--depths", hop_latency::DEFAULT_DEPTHS)
            };
            let rate = number(args, "--rate").unwrap_or(LADDER_DEFAULT_RATE);
            let duration = secs(args, "--duration-secs").unwrap_or(LADDER_DEFAULT_DURATION);
            let tuning = tuning(args, EngineTuning::multi_hop());

            let results =
                runtime().block_on(hop_latency::run_ladder(&depths, rate, duration, &tuning));

            let mut ok = true;
            for result in &results {
                println!("{}", result.to_json());
                if result.e2e_count == 0 {
                    eprintln!(
                        "HARNESS FAILURE: depth {} observed zero end-to-end samples \
                         (commits_issued={}, changes_applied_terminal={}) — this depth's T1 \
                         result is meaningless, not merely failing",
                        result.depth, result.commits_issued, result.changes_applied_terminal
                    );
                    ok = false;
                } else if !result.changes_match_committed {
                    eprintln!(
                        "HARNESS FAILURE: depth {} applied {} changes for {} committed rows — \
                         the metric cross-check failed, so its latency numbers describe only \
                         the subset that was observed",
                        result.depth, result.changes_applied_terminal, result.rows_issued
                    );
                    ok = false;
                }
                if !result.oracle.ok() {
                    eprintln!(
                        "CORRECTNESS FAILURE: depth {}'s terminal hop disagrees with the \
                         source in {} row(s)",
                        result.depth, result.oracle.mismatched_rows
                    );
                    ok = false;
                }
                if let Some(increment) = result.per_hop_increment_ms {
                    eprintln!(
                        "depth {}: per-hop increment {:.1}ms, T1 {}",
                        result.depth,
                        increment,
                        if result.t1_all_pass { "PASS" } else { "FAIL" }
                    );
                }
            }
            Some(ok)
        }

        "throughput-ramp" => {
            let rates = number_list(args, "--rates").unwrap_or_else(|| RAMP_DEFAULT_RATES.to_vec());
            let offer = offer(args, RAMP_DEFAULT_DURATION, RAMP_DEFAULT_GRACE);
            let tuning = throughput_tuning(args);

            let probes = runtime().block_on(throughput::run_ramp(&rates, offer, &tuning));
            let ok = report_probes(&probes, "throughput-ramp");
            match throughput::knee(&probes) {
                Some(knee) => eprintln!(
                    "knee: {} rows/sec kept (applied {} while offered, achieved {:.0}/sec{}){}",
                    knee.target_rows_per_sec,
                    human_rate(knee.in_window_applied),
                    knee.achieved_rows_per_sec,
                    if knee.generator_bound {
                        " — GENERATOR-BOUND, so the knee is a floor"
                    } else {
                        ""
                    },
                    match probes.last() {
                        Some(last) if last.stops_ramp() => format!(
                            ", {} rows/sec not ({})",
                            last.target_rows_per_sec,
                            probe_outcome(last)
                        ),
                        _ =>
                            ", no tested rate failed — extend --rates to find the knee".to_string(),
                    }
                ),
                None => match probes.first() {
                    Some(first) => eprintln!(
                        "no candidate rate kept its target — even the lowest tested rate ({} \
                         rows/sec) didn't ({})",
                        first.target_rows_per_sec,
                        probe_outcome(first)
                    ),
                    None => eprintln!("no candidate rates given"),
                },
            }
            Some(ok)
        }

        "transaction-shape" => {
            let shapes = usize_list(args, "--shapes", throughput::DEFAULT_SHAPES);
            let target_rate = number(args, "--target-rate").unwrap_or(SHAPE_DEFAULT_TARGET_RATE);
            let offer = offer(args, SHAPE_DEFAULT_DURATION, SHAPE_DEFAULT_GRACE);
            let tuning = throughput_tuning(args);

            let probes = runtime().block_on(throughput::run_shape_sweep(
                &shapes,
                target_rate,
                offer,
                &tuning,
            ));
            Some(report_probes(&probes, "transaction-shape"))
        }

        "fold-in-ratio" => {
            let ratios = usize_list(args, "--ratios", fold_in::DEFAULT_RATIOS);
            let target_rate = number(args, "--target-rate").unwrap_or(FOLD_IN_DEFAULT_TARGET_RATE);
            let offer = offer(args, FOLD_IN_DEFAULT_DURATION, FOLD_IN_DEFAULT_GRACE);
            let tuning = throughput_tuning(args);

            let results =
                runtime().block_on(fold_in::run_sweep(&ratios, target_rate, offer, &tuning));
            Some(report_fold_in(&results, name))
        }

        "group-contention" => {
            let groups = usize_list(args, "--groups", CONTENTION_DEFAULT_GROUPS);
            let threads = usize_list(
                args,
                "--threads",
                &[throughput::THROUGHPUT_APPLICATION_THREADS],
            );
            let target_rate = number(args, "--target-rate").unwrap_or(FOLD_IN_DEFAULT_TARGET_RATE);
            let offer = offer(args, FOLD_IN_DEFAULT_DURATION, FOLD_IN_DEFAULT_GRACE);
            let tuning = throughput_tuning(args);

            // One probe at a time, reported as it lands: a full grid runs for
            // tens of minutes, and a partial one is still worth reading.
            let runtime = runtime();
            let mut ok = true;
            for &g in &groups {
                for &application_threads in &threads {
                    let tuning = EngineTuning {
                        application_threads,
                        ..tuning.clone()
                    };
                    let result =
                        runtime.block_on(fold_in::run_probe(g, target_rate, offer, &tuning));
                    ok &= report_fold_in(std::slice::from_ref(&result), name);
                    report_contention(&result);
                }
            }
            Some(ok)
        }

        // #617's large aggregate build under write load (#620, #629).
        "build-under-load" => {
            let cfg = build_under_load_config(args);
            let tuning = throughput_tuning(args);
            let result = runtime().block_on(build_under_load::run(cfg, &tuning));
            println!("{}", result.to_json(name));
            eprintln!("{}", result.human());
            if !result.oracle_ok {
                eprintln!(
                    "CORRECTNESS FAILURE: {} groups disagree with the oracle",
                    result.oracle_mismatched_groups
                );
            }
            Some(result.oracle_ok && result.writes.errors == 0)
        }

        "write-tax" => {
            let variants = variants(args, &write_tax::DEFAULT_VARIANTS);
            let shapes: Vec<Shape> = match flag(args, "--shapes") {
                Some(raw) => raw.split(',').map(Shape::parse).collect(),
                None => write_tax::DEFAULT_SHAPES
                    .iter()
                    .map(|s| Shape::parse(s))
                    .collect(),
            };
            let reps = reps(args);
            let opts = cell_options(args);
            let results = writer_runtime().block_on(write_tax::run_matrix(
                "write-tax",
                &variants,
                &shapes,
                reps,
                &opts,
            ));
            eprintln!("{}", write_tax::summary(&results));
            Some(results.iter().all(|r| r.ring_ok))
        }

        "capture-ceiling" => {
            let variants = variants(args, capture_ceiling::DEFAULT_VARIANTS);
            let writers = usize_list(args, "--writers", capture_ceiling::DEFAULT_WRITERS);
            let rows_per_commit = number(args, "--rows-per-commit")
                .map(|v| v as usize)
                .unwrap_or(capture_ceiling::DEFAULT_ROWS_PER_COMMIT);
            let reps = reps(args);
            let opts = cell_options(args);
            let results = writer_runtime().block_on(capture_ceiling::run(
                &variants,
                rows_per_commit,
                &writers,
                reps,
                &opts,
            ));
            eprintln!("{}", write_tax::summary(&results));
            Some(results.iter().all(|r| r.ring_ok))
        }

        // #623 D8a: `--workloads serial1,serial-batch,random1,update1`,
        // `--writers 1,4,8,16`, `--variants none,trigger`,
        // `--isolation serializable[,read-committed]`, `--batch 10`,
        // `--secs 8`, `--reps`.
        "ssi-tax" => {
            let workloads: Vec<ssi_tax::Workload> = match flag(args, "--workloads") {
                Some(raw) => raw.split(',').map(ssi_tax::Workload::parse).collect(),
                None => ssi_tax::DEFAULT_WORKLOADS.to_vec(),
            };
            let isolations: Vec<ssi_tax::Isolation> = match flag(args, "--isolation") {
                Some(raw) => raw.split(',').map(ssi_tax::Isolation::parse).collect(),
                None => vec![ssi_tax::Isolation::Serializable],
            };
            let writers = usize_list(args, "--writers", ssi_tax::DEFAULT_WRITERS);
            let variants = variants(args, ssi_tax::DEFAULT_VARIANTS);
            let opts = ssi_tax::Options {
                batch: number(args, "--batch")
                    .map(|v| v as usize)
                    .unwrap_or(ssi_tax::DEFAULT_BATCH),
                window: secs(args, "--secs").unwrap_or(ssi_tax::DEFAULT_WINDOW),
            };
            let cells = ssi_tax::schedule(&workloads, &writers, &variants, &isolations, reps(args));
            let results = writer_runtime().block_on(ssi_tax::run_matrix(cells, &opts));
            eprintln!("{}", ssi_tax::summary(&results));
            Some(true)
        }

        "idle-cost" => {
            let warmup = secs(args, "--warmup-secs").unwrap_or(IDLE_DEFAULT_WARMUP);
            let duration = secs(args, "--duration-secs").unwrap_or(IDLE_DEFAULT_DURATION);
            let tuning = throughput_tuning(args);

            let statements = args.iter().any(|a| a == "--statements");

            let result = runtime().block_on(idle_cost::run(warmup, duration, &tuning, statements));
            println!("{}", result.to_json());
            if let Some(rows) = &result.statements_json {
                println!("{rows}");
            }
            eprintln!(
                "idle: {:.1} transactions/sec, {:.0} WAL bytes/sec ({:.2} KB/s), {:.2} seals/sec",
                result.xact_commit_per_sec,
                result.wal_bytes_per_sec,
                result.wal_bytes_per_sec / 1024.0,
                result.seals_per_sec,
            );
            Some(true)
        }

        "generator-reach" => {
            let connections = connections(args).unwrap_or(load::DEFAULT_CONNECTIONS);
            let rows_per_commit = number(args, "--rows-per-commit")
                .map(|v| v as usize)
                .unwrap_or(REACH_DEFAULT_ROWS_PER_COMMIT);
            let duration = secs(args, "--duration-secs").unwrap_or(REACH_DEFAULT_DURATION);

            let result =
                runtime().block_on(generator_reach::run(connections, rows_per_commit, duration));
            println!("{}", result.to_json());
            Some(true)
        }

        _ => None,
    }
}

/// Issue #277's one-line reading of a probe's [`contention`] sample.
fn report_contention(result: &fold_in::FoldInResult) {
    let c = &result.contention;
    if c.samples == 0 {
        // All-zero means over no samples would read as "no contention".
        eprintln!(
            "{} groups x {} drain workers: no contention samples landed in the window — \
             lock-wait attribution unavailable, not zero",
            result.groups, result.application_threads
        );
        return;
    }
    eprintln!(
        "{} groups x {} drain workers: folded {} while offered; engine busy {:.2} backends, \
         {:.2} waiting on row locks ({:.0}%, {:.2} in the aggregate pre-lock), {:.2} running, \
         {:.2} idle in txn; {} deadlocks, {} rollbacks",
        result.groups,
        result.application_threads,
        human_rate(result.in_window_folded),
        c.engine_busy_mean,
        c.engine_row_lock_mean,
        c.row_lock_share() * 100.0,
        c.prelock_wait_mean,
        c.engine_running_mean,
        c.engine_idle_in_txn_mean,
        result.deadlocks,
        result.xact_rollbacks,
    );
    for (class, statement, mean) in &c.top_statements {
        eprintln!("    {mean:>6.2} {:<11} {statement}", class.label());
    }
    eprintln!(
        "    page lock hold p50/p99/max {}/{}/{} ms over {} page txns; page lock wait \
         p50/p99/max {}/{}/{} ms over {} waits; {}",
        disk_tier::json_ms(c.page_lock_holds.p50_ms),
        disk_tier::json_ms(c.page_lock_holds.p99_ms),
        disk_tier::json_ms(c.page_lock_holds.max_ms),
        c.page_lock_holds.count,
        disk_tier::json_ms(c.page_lock_waits.p50_ms),
        disk_tier::json_ms(c.page_lock_waits.p99_ms),
        disk_tier::json_ms(c.page_lock_waits.max_ms),
        c.page_lock_waits.count,
        result.server.human(result.folded_rows_in_window),
    );
}

/// Prints one JSON line per aggregate probe, flags oracle failures, and
/// states each probe's T3 verdict — or that it measured the generator.
fn report_fold_in(results: &[fold_in::FoldInResult], scenario: &str) -> bool {
    let mut ok = true;
    for result in results {
        println!("{}", result.to_json(scenario));
        eprintln!(
            "{scenario}: {} groups, {} application threads: {}, {:.0} WAL bytes/row",
            result.groups,
            result.application_threads,
            result.disk.human(),
            result.wal_bytes_per_row
        );
        match result.oracle_ok {
            Some(true) => {}
            Some(false) => {
                eprintln!(
                    "CORRECTNESS FAILURE: {} groups — {} of {} groups disagree with the oracle",
                    result.groups,
                    result
                        .oracle_mismatched_groups
                        .map_or_else(|| "?".to_string(), |n| n.to_string()),
                    result.oracle_groups
                );
                ok = false;
            }
            // Not a failure — a partially drained aggregate legitimately
            // holds partial sums — but not a pass either (#335).
            None => eprintln!(
                "oracle SKIPPED: {} groups — the target never folded in every row within the \
                 grace period, so this probe's correctness was not checked",
                result.groups
            ),
        }
        // Same cross-check as `report_probes` (#423): the counter counts
        // staged source rows, one per committed row, so any excess is a
        // mid-window re-stage the probe measured on top of its offer.
        if restaged_in_window(result.changes_applied, result.rows_issued) {
            eprintln!(
                "HARNESS FAILURE: {} groups at {} rows/sec applied {} changes for {} committed \
                 rows — source rows were staged again during the window (a catch-up \
                 backfill?), so this rate was measured under extra load",
                result.groups,
                result.target_rows_per_sec,
                result.changes_applied,
                result.rows_issued
            );
            ok = false;
        }
        if result.generator_bound {
            eprintln!(
                "GENERATOR-BOUND: {} groups — generator offered only {:.0} of {} rows/sec and \
                 the aggregate kept up with all of it, so this row says nothing about T3 at the \
                 target; raise --connections",
                result.groups, result.achieved_rows_per_sec, result.target_rows_per_sec
            );
        } else {
            eprintln!(
                "fold-in {:.0}:1 ({} groups) — T3 {} at {} rows/sec: folded {} while offered, {} \
                 (offered {:.0}/sec)",
                result.fold_in_ratio,
                result.groups,
                if result.kept_target_rate { "YES" } else { "NO" },
                result.target_rows_per_sec,
                human_rate(result.in_window_folded),
                match result.folded_rows_per_sec {
                    Some(rate) => format!("{rate:.0} rows/sec end to end"),
                    None => "never caught up within the grace period".to_string(),
                },
                result.achieved_rows_per_sec,
            );
        }
    }
    ok
}

/// Why a throughput probe did or didn't keep its target rate, for a
/// human-readable line.
fn probe_outcome(probe: &throughput::ThroughputProbe) -> String {
    let applied = human_rate(probe.in_window_applied);
    let outcome = if probe.drained {
        format!("applied {applied} while offered, drained within grace")
    } else {
        format!(
            "applied {applied} while offered, backlog {} after grace",
            probe.backlog_after_grace
        )
    };
    // A rate that reads as kept would otherwise sit next to a NO (#509).
    if probe.counter_disagrees() {
        format!("void, the applied-changes counter disagrees with the rows committed; {outcome}")
    } else {
        outcome
    }
}

/// Prints one JSON line per probe, flags correctness/cross-check failures,
/// and states each probe's verdict — or that it measured the generator.
fn report_probes(probes: &[throughput::ThroughputProbe], scenario: &str) -> bool {
    let mut ok = true;
    for probe in probes {
        println!("{}", probe.to_json(scenario));
        if !probe.oracle.ok() {
            eprintln!(
                "CORRECTNESS FAILURE: {} at {} rows/sec ({} rows/commit) — {} target row(s) \
                 disagree with the source",
                scenario,
                probe.target_rows_per_sec,
                probe.rows_per_commit,
                probe.oracle.mismatched_rows
            );
            ok = false;
        }
        // A drained probe landed its whole backlog, so every committed row
        // must have been applied *and* counted: a shortfall there is the
        // #266 metric cross-check failing, not a slow pipeline.
        if throughput::counter_short_of_drain(
            probe.drained,
            probe.changes_applied,
            probe.rows_issued,
        ) {
            eprintln!(
                "HARNESS FAILURE: {} at {} rows/sec drained but applied only {} of {} \
                 committed rows",
                scenario, probe.target_rows_per_sec, probe.changes_applied, probe.rows_issued
            );
            ok = false;
        }
        // The other direction (#423): the counter counts staged rows, so it
        // can only pass rows committed if something staged a source row a
        // second time inside the window, and then the probe measured more
        // work than it offered. Setup waits out the one known source of that,
        // the catch-up backfill a definition parks when it goes live.
        if restaged_in_window(probe.changes_applied, probe.rows_issued) {
            eprintln!(
                "HARNESS FAILURE: {} at {} rows/sec applied {} changes for {} committed rows — \
                 source rows were staged again during the window (a catch-up backfill?), so \
                 this rate was measured under extra load",
                scenario, probe.target_rows_per_sec, probe.changes_applied, probe.rows_issued
            );
            ok = false;
        }
        if probe.generator_bound {
            eprintln!(
                "GENERATOR-BOUND: {} at {} rows/sec — generator offered only {:.0}/sec from {} \
                 connections and the engine kept up with all of it; this row measured the \
                 generator, not the engine (raise --connections)",
                scenario, probe.target_rows_per_sec, probe.achieved_rows_per_sec, probe.connections
            );
        } else {
            eprintln!(
                "{} at {} rows/sec ({} rows/commit) — kept target rate {}: {} (offered {:.0}/sec)",
                scenario,
                probe.target_rows_per_sec,
                probe.rows_per_commit,
                if probe.kept_target_rate { "YES" } else { "NO" },
                probe_outcome(probe),
                probe.achieved_rows_per_sec,
            );
        }
    }
    ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::tuning::{
        STOCK_MAINTENANCE_INTERVAL, STOCK_POLL_INTERVAL, STOCK_RECONCILE_INTERVAL,
    };

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn tuning_defaults_to_stock_when_no_flags_are_given() {
        let t = tuning(&argv(&["hop-ladder"]), EngineTuning::default());
        assert_eq!(t.poll_interval, STOCK_POLL_INTERVAL);
        assert_eq!(t.maintenance_interval, STOCK_MAINTENANCE_INTERVAL);
        assert_eq!(t.reconcile_interval, STOCK_RECONCILE_INTERVAL);
        assert_eq!(t.application_threads, 4);
    }

    #[test]
    fn the_ladder_runs_the_stock_reconcile_pass_but_yields_to_the_flag() {
        assert_eq!(
            tuning(&argv(&["hop-ladder"]), EngineTuning::multi_hop()).reconcile_interval,
            STOCK_RECONCILE_INTERVAL
        );
        assert_eq!(
            tuning(
                &argv(&["hop-ladder", "--reconcile-interval-ms", "250"]),
                EngineTuning::multi_hop()
            )
            .reconcile_interval,
            Duration::from_millis(250)
        );
    }

    #[test]
    fn tuning_reads_the_interval_flags() {
        let t = tuning(
            &argv(&[
                "hop-latency",
                "--poll-interval-ms",
                "20",
                "--maintenance-interval-ms",
                "10",
                "--application-threads",
                "8",
            ]),
            EngineTuning::default(),
        );
        assert_eq!(t.poll_interval, Duration::from_millis(20));
        assert_eq!(t.maintenance_interval, Duration::from_millis(10));
        assert_eq!(t.application_threads, 8);
    }

    #[test]
    fn tuning_reads_the_drain_batch_cap_or_keeps_the_stock_default() {
        assert_eq!(
            tuning(&argv(&["build-under-load"]), EngineTuning::default()).drain_batch_cap,
            trellis::ClientOptions::default().drain_batch_cap
        );
        assert_eq!(
            tuning(
                &argv(&["build-under-load", "--drain-batch-cap", "5000"]),
                EngineTuning::default()
            )
            .drain_batch_cap,
            5000
        );
    }

    #[test]
    fn tuning_reads_the_build_chunk_rows_or_keeps_the_default() {
        let stock = tuning(&argv(&["build-under-load"]), EngineTuning::default());
        assert_eq!(
            stock.build_chunk_rows,
            trellis::ClientOptions::default().build_chunk_rows
        );
        let t = tuning(
            &argv(&["build-under-load", "--build-chunk-rows", "50000"]),
            EngineTuning::default(),
        );
        assert_eq!(t.build_chunk_rows, 50_000);
        assert_eq!(t.client_options().build_chunk_rows, 50_000);
    }

    #[test]
    fn build_under_load_takes_zero_writers() {
        let c = build_under_load_config(&argv(&["build-under-load", "--writers", "0"]));
        assert_eq!(c.writers, 0);
    }

    #[test]
    fn throughput_tuning_defaults_to_eight_drain_workers() {
        assert_eq!(
            throughput_tuning(&argv(&["throughput-ramp"])).application_threads,
            throughput::THROUGHPUT_APPLICATION_THREADS
        );
    }

    #[test]
    fn lists_parse_and_fall_back_to_their_defaults() {
        assert_eq!(
            usize_list(
                &argv(&["hop-ladder", "--depths", "1, 2,10"]),
                "--depths",
                &[3]
            ),
            vec![1, 2, 10]
        );
        assert_eq!(
            usize_list(
                &argv(&["hop-ladder"]),
                "--depths",
                hop_latency::DEFAULT_DEPTHS
            ),
            hop_latency::DEFAULT_DEPTHS.to_vec()
        );
        assert_eq!(
            number_list(&argv(&["r", "--rates", "1000,2.5e5"]), "--rates"),
            Some(vec![1000.0, 250_000.0])
        );
    }

    #[test]
    fn offer_defaults_to_the_parallel_generators_connection_count() {
        let o = offer(
            &argv(&["fold-in-ratio"]),
            Duration::from_secs(20),
            Duration::from_secs(120),
        );
        assert_eq!(o.connections, load::DEFAULT_CONNECTIONS);
        assert_eq!(o.duration, Duration::from_secs(20));
        assert_eq!(o.grace, Duration::from_secs(120));

        let o = offer(
            &argv(&["fold-in-ratio", "--connections", "16", "--grace-secs", "5"]),
            Duration::from_secs(20),
            Duration::from_secs(120),
        );
        assert_eq!(o.connections, 16);
        assert_eq!(o.grace, Duration::from_secs(5));
    }

    #[test]
    #[should_panic(expected = "--connections must be at least 1")]
    fn zero_connections_is_rejected() {
        connections(&argv(&["generator-reach", "--connections", "0"]));
    }

    fn fit(rows_per_sec: f64) -> crate::streaming::rate::InWindowRate {
        crate::streaming::rate::InWindowRate {
            rows_per_sec,
            tolerance: load::GENERATOR_UNDERSHOOT_TOLERANCE,
        }
    }

    fn probe(rows_issued: u64, changes_applied: u64) -> throughput::ThroughputProbe {
        throughput::ThroughputProbe {
            target_rows_per_sec: 20_000.0,
            rows_per_commit: 400,
            commits_per_sec: 50.0,
            connections: 8,
            offered_duration_secs: 10.0,
            commit_tail_secs: 0.0,
            rows_issued,
            achieved_rows_per_sec: 20_000.0,
            generator_bound: false,
            changes_applied,
            backlog_after_grace: 0,
            drained: true,
            in_window_applied: Some(fit(20_000.0)),
            // The scenario's own verdict, so the fixture can't disagree with
            // what `report_probes` makes of its counter (#509).
            kept_target_rate: throughput::probe_kept_target_rate(
                true,
                Some(fit(20_000.0)),
                20_000.0,
                20_000.0,
                changes_applied,
                rows_issued,
            ),
            e2e_count: rows_issued,
            e2e_p50_bucket_frac: 1.0,
            e2e_p99_bucket_frac: 1.0,
            e2e_max_frac: 1.0,
            oracle: crate::streaming::chain::ChainOracle {
                source_rows: rows_issued as i64 + 1,
                terminal_rows: rows_issued as i64 + 1,
                mismatched_rows: 0,
                fully_converged: true,
            },
        }
    }

    fn fold_in(rows_issued: u64, changes_applied: u64) -> fold_in::FoldInResult {
        fold_in::FoldInResult {
            fold_in_ratio: 1000.0,
            groups: 50,
            target_rows_per_sec: 50_000.0,
            application_threads: 8,
            connections: 8,
            offered_duration_secs: 10.0,
            commit_tail_secs: 0.0,
            rows_issued,
            achieved_rows_per_sec: 50_000.0,
            generator_bound: false,
            changes_applied,
            drained: true,
            folded_rows_per_sec: Some(50_000.0),
            in_window_folded: Some(fit(50_000.0)),
            kept_target_rate: fold_in::fold_in_kept_target_rate(
                true,
                Some(fit(50_000.0)),
                50_000.0,
                50_000.0,
                changes_applied,
                rows_issued,
            ),
            e2e_count: rows_issued,
            e2e_p50_bucket_frac: 1.0,
            e2e_p99_bucket_frac: 1.0,
            e2e_max_frac: 1.0,
            oracle_ok: Some(true),
            oracle_groups: 50,
            oracle_mismatched_groups: Some(0),
            contention: Default::default(),
            deadlocks: 0,
            xact_rollbacks: 0,
            disk: Default::default(),
            wal_bytes_per_row: 0.0,
            folded_rows_in_window: rows_issued,
            server: Default::default(),
        }
    }

    /// Issue #423: a counter ahead of the rows committed means something
    /// re-staged source rows inside the window (the go-live catch-up measured
    /// 297,601 for 200,000), so the probe fails even though it drained and
    /// its oracle agrees.
    #[test]
    fn a_counter_ahead_of_rows_committed_fails_the_probe() {
        assert!(report_probes(&[probe(200_000, 200_000)], "throughput-ramp"));
        assert!(!report_probes(
            &[probe(200_000, 297_601)],
            "throughput-ramp"
        ));
        let undrained = throughput::ThroughputProbe {
            drained: false,
            kept_target_rate: false,
            backlog_after_grace: 10,
            ..probe(200_000, 200_001)
        };
        assert!(!report_probes(&[undrained], "throughput-ramp"));

        assert!(report_fold_in(
            &[fold_in(500_000, 500_000)],
            "fold-in-ratio"
        ));
        assert!(!report_fold_in(
            &[fold_in(500_000, 596_001)],
            "fold-in-ratio"
        ));
    }

    /// Issue #509: a counter that fails the run (#423's re-stage, or #266's
    /// drained-but-short cross-check) must fail the probe's JSON verdict too,
    /// or anything reading the JSON alone sees `kept_target_rate: true` from a
    /// run that exited non-zero. Every other check passes in these fixtures,
    /// so the run's outcome and the JSON verdict must be the same answer.
    #[test]
    fn the_json_verdict_agrees_with_the_counter_cross_checks() {
        for (rows_issued, changes_applied) in
            [(200_000, 200_000), (200_000, 297_601), (200_000, 199_999)]
        {
            let probes = [probe(rows_issued, changes_applied)];
            let run_ok = report_probes(&probes, "throughput-ramp");
            let json = probes[0].to_json("throughput-ramp");
            assert_eq!(
                json.contains("\"kept_target_rate\":true"),
                run_ok,
                "applied {changes_applied} of {rows_issued}: {json}"
            );
        }
        for (rows_issued, changes_applied) in [(500_000, 500_000), (500_000, 596_001)] {
            let results = [fold_in(rows_issued, changes_applied)];
            let run_ok = report_fold_in(&results, "fold-in-ratio");
            let json = results[0].to_json("fold-in-ratio");
            assert_eq!(
                json.contains("\"kept_target_rate\":true"),
                run_ok,
                "applied {changes_applied} of {rows_issued}: {json}"
            );
        }
    }

    /// Issue #509: a probe voided by its counter reads as failed, but it
    /// measured nothing, so the ramp steps past it and finds the knee above
    /// it instead of reporting the rate below it.
    #[test]
    fn a_void_probe_neither_stops_the_ramp_nor_is_the_knee() {
        let at = |target: f64, applied: f64, rows_issued: u64, changes_applied: u64| {
            throughput::ThroughputProbe {
                target_rows_per_sec: target,
                achieved_rows_per_sec: target,
                in_window_applied: Some(fit(applied)),
                kept_target_rate: throughput::probe_kept_target_rate(
                    true,
                    Some(fit(applied)),
                    target,
                    target,
                    changes_applied,
                    rows_issued,
                ),
                ..probe(rows_issued, changes_applied)
            }
        };
        let probes = [
            at(50_000.0, 50_000.0, 500_000, 500_000),
            // #423: re-staged mid-window, so the counter ran past the rows.
            at(100_000.0, 100_000.0, 1_000_000, 1_400_000),
            // #266: drained, but the counter missed rows.
            at(125_000.0, 125_000.0, 1_250_000, 1_200_000),
            at(150_000.0, 150_000.0, 1_500_000, 1_500_000),
            // Genuinely slow: half its target while offered.
            at(200_000.0, 100_000.0, 2_000_000, 2_000_000),
        ];
        for void in &probes[1..3] {
            assert!(!void.kept_target_rate);
            assert!(void.counter_disagrees());
            assert!(
                probe_outcome(void).starts_with("void"),
                "{}",
                probe_outcome(void)
            );
        }
        let stops: Vec<bool> = probes.iter().map(|p| p.stops_ramp()).collect();
        assert_eq!(stops, [false, false, false, false, true]);
        let knee = throughput::knee(&probes).expect("150k kept its rate");
        assert_eq!(knee.target_rows_per_sec, 150_000.0);
    }

    /// Issue #319: a probe applying at half its target drains a short
    /// window's backlog inside the grace period. That used to be the knee;
    /// the in-window rate now keeps it out.
    #[test]
    fn a_probe_that_drains_at_half_its_target_rate_is_not_the_knee() {
        let kept = throughput::ThroughputProbe {
            target_rows_per_sec: 50_000.0,
            achieved_rows_per_sec: 50_000.0,
            in_window_applied: Some(fit(50_000.0)),
            ..probe(1_000_000, 1_000_000)
        };
        let half_rate = throughput::ThroughputProbe {
            target_rows_per_sec: 100_000.0,
            achieved_rows_per_sec: 100_000.0,
            in_window_applied: Some(fit(50_000.0)),
            kept_target_rate: crate::streaming::rate::kept_target_rate(
                true,
                Some(fit(50_000.0)),
                100_000.0,
                100_000.0,
            ),
            ..probe(2_000_000, 2_000_000)
        };
        assert!(half_rate.drained);
        assert!(!half_rate.kept_target_rate);
        let probes = [kept, half_rate];
        let knee = throughput::knee(&probes).expect("the 50k probe kept its rate");
        assert_eq!(knee.target_rows_per_sec, 50_000.0);
        assert!(probe_outcome(&probes[1]).contains("drained within grace"));
    }

    /// Issue #335: an oracle that never ran reads as skipped — `null`, not a
    /// `0`-mismatch pass — and isn't reported as a correctness failure either.
    #[test]
    fn a_skipped_aggregate_oracle_reads_as_skipped() {
        let skipped = fold_in::FoldInResult {
            drained: false,
            kept_target_rate: false,
            folded_rows_per_sec: None,
            oracle_ok: None,
            oracle_mismatched_groups: None,
            ..fold_in(500_000, 400_000)
        };
        let json = skipped.to_json("fold-in-ratio");
        assert!(json.contains("\"oracle_ok\":null"), "{json}");
        assert!(json.contains("\"oracle_mismatched_groups\":null"), "{json}");
        assert!(json.contains("\"drained\":false"), "{json}");
        assert!(report_fold_in(&[skipped], "fold-in-ratio"));

        let checked = fold_in(500_000, 500_000).to_json("fold-in-ratio");
        assert!(checked.contains("\"oracle_ok\":true"), "{checked}");
        assert!(
            checked.contains("\"oracle_mismatched_groups\":0"),
            "{checked}"
        );
    }

    #[test]
    fn a_throughput_probe_reports_its_in_window_rate_and_verdict() {
        let json = throughput::ThroughputProbe {
            in_window_applied: None,
            kept_target_rate: false,
            ..probe(200_000, 200_000)
        }
        .to_json("transaction-shape");
        assert!(
            json.contains("\"in_window_applied_rows_per_sec\":null,\"rate_tolerance\":null"),
            "{json}"
        );
        assert!(json.contains("\"kept_target_rate\":false"), "{json}");
        assert!(json.contains("\"drained\":true"), "{json}");
        assert!(!json.contains("sustained"), "{json}");
    }

    #[test]
    fn build_under_load_defaults_and_flags() {
        let d = build_under_load_config(&argv(&["build-under-load"]));
        assert_eq!(
            (d.rows, d.groups, d.loaders, d.writers, d.write_rate),
            (10_000_000, 100_000, 4, 8, 2_000.0)
        );
        assert_eq!(d.pre_define, Duration::from_secs(2));
        assert_eq!(d.post_live, Duration::from_secs(20));
        assert_eq!(d.build_timeout, Duration::from_secs(3600));
        assert_eq!(d.grace, Duration::from_secs(600));
        assert_eq!(d.oracle_poll_min, Duration::from_secs(5));
        assert_eq!(d.progress, Some(Duration::from_secs(30)));
        assert!(!d.min_max);
        assert!(
            build_under_load_config(&argv(&["build-under-load", "--min-max"])).min_max,
            "--min-max adds MIN and MAX fields"
        );
        assert!(!d.one_to_one);
        assert!(
            build_under_load_config(&argv(&["build-under-load", "--one-to-one"])).one_to_one,
            "--one-to-one builds a 1-1 target"
        );
        assert!(!d.apply_latency && !d.apply_big_to_side);
        let a = build_under_load_config(&argv(&[
            "build-under-load",
            "--apply-latency",
            "--apply-latency-big-to-side",
        ]));
        assert!(a.apply_latency && a.apply_big_to_side);
        let c = build_under_load_config(&argv(&[
            "build-under-load",
            "--rows",
            "200000",
            "--groups",
            "1000",
            "--writers",
            "2",
            "--write-rate",
            "500",
            "--duration-secs",
            "5",
            "--grace-secs",
            "60",
            "--oracle-poll-min-secs",
            "2.5",
            "--progress-secs",
            "0",
        ]));
        assert_eq!(c.oracle_poll_min, Duration::from_millis(2500));
        assert_eq!((c.rows, c.groups, c.writers), (200_000, 1000, 2));
        assert_eq!(c.write_rate, 500.0);
        assert_eq!(c.post_live, Duration::from_secs(5));
        assert_eq!(c.grace, Duration::from_secs(60));
        assert_eq!(c.progress, None);
    }

    #[test]
    fn write_tax_flags_parse_over_the_cell_defaults() {
        let opts = cell_options(&argv(&["write-tax"]));
        let default = CellOptions::default();
        assert_eq!(opts.max_window, default.max_window);
        assert_eq!(opts.rows, None);
        assert_eq!(opts.probe, ProbeMode::Orm);
        assert_eq!(reps(&argv(&["write-tax"])), WRITE_TAX_DEFAULT_REPS);

        let args = argv(&[
            "write-tax",
            "--max-secs",
            "10",
            "--rows",
            "5000",
            "--copy-rows",
            "10000000",
            "--snapshot-probe",
            "all",
            "--reps",
            "1",
            "--variants",
            "none,trigger",
        ]);
        let opts = cell_options(&args);
        assert_eq!(opts.max_window, Duration::from_secs(10));
        assert_eq!(opts.rows, Some(5000));
        assert_eq!(opts.copy_rows, 10_000_000);
        assert_eq!(opts.probe, ProbeMode::All);
        assert_eq!(reps(&args), 1);
        assert_eq!(
            variants(&args, &Variant::ALL),
            [Variant::None, Variant::Trigger]
        );
        assert_eq!(
            variants(
                &argv(&["capture-ceiling"]),
                capture_ceiling::DEFAULT_VARIANTS
            ),
            capture_ceiling::DEFAULT_VARIANTS
        );
    }

    #[test]
    fn an_unknown_name_is_not_handled_here() {
        assert_eq!(run("high-cardinality", &argv(&["high-cardinality"])), None);
    }
}
