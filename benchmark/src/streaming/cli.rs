//! Argument parsing and dispatch for the streaming scenarios.
//!
//! Kept out of `main.rs` so the backfill scenarios there stay readable: these
//! six scenarios carry far more knobs than the `--n`/`--g`/`--ceiling-secs`
//! shape `main.rs`'s own parser handles, and they all share the same
//! [`EngineTuning`] flags.

use std::time::Duration;

use crate::streaming::tuning::EngineTuning;
use crate::streaming::{fold_in, hop_latency, idle_cost, intake_ceiling, throughput};

/// Every scenario name this module handles, for `main.rs`'s usage message.
pub const SCENARIOS: &[&str] = &[
    "hop-ladder",
    "hop-latency",
    "throughput-ramp",
    "transaction-shape",
    "fold-in-ratio",
    "intake-ceiling",
    "idle-cost",
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
/// measured single-hop knee, so a `sustained: false` there means the *shape*
/// hurt rather than that the rate alone had already saturated the pipeline
/// regardless of shape.
const SHAPE_DEFAULT_TARGET_RATE: f64 = 20_000.0;
const SHAPE_DEFAULT_DURATION: Duration = Duration::from_secs(20);
const SHAPE_DEFAULT_GRACE: Duration = Duration::from_secs(30);

/// The fold-in sweep's defaults: #266's own ratio list at T3's 400k rows/sec.
const FOLD_IN_DEFAULT_TARGET_RATE: f64 = 400_000.0;
const FOLD_IN_DEFAULT_DURATION: Duration = Duration::from_secs(20);
/// Longer than every other scenario's 30s grace (issue #270 review): at
/// 400k rows/sec offered against a single-connection generator that tops out
/// well below that (see `load`'s own doc comment, #276), every ratio starts
/// the grace window already backlogged, and a low fold-in ratio's target
/// write count is close to the row count — near the same write load as a 1-1
/// chain, whose own knee sits at ~210-230k rows/sec. Measured directly: ratio
/// 1000:1 needs on the order of 90-120s to fully drain and pass its oracle;
/// 30s reports every ratio as `sustained: false` with `oracle_ok: null`
/// (unevaluated, not failed — see `check_aggregate_oracle`), which is not a
/// real reproduction of a T3 measurement. A low ratio (e.g. 10:1, 40,000
/// groups at this target rate) may still not drain even at 120s — that is
/// this scenario's own hypothesis under test (throughput should scale with
/// fold-in ratio), not a harness fault, and is why this raises the grace
/// rather than lowering the default target rate.
const FOLD_IN_DEFAULT_GRACE: Duration = Duration::from_secs(120);

const INTAKE_DEFAULT_ROWS_PER_COMMIT: usize = 1000;
const INTAKE_DEFAULT_DURATION: Duration = Duration::from_secs(20);
const INTAKE_DEFAULT_GRACE: Duration = Duration::from_secs(30);

/// Idle cost's defaults: 8 drain threads (#269's own "staging worker + 8 drain
/// threads" wording), a 10 s warmup so start-of-day work never pollutes the
/// steady-state sample, then a 60 s window — long enough that a few hundred
/// transactions/sec is measured over tens of thousands of transactions rather
/// than a handful.
const IDLE_DEFAULT_WARMUP: Duration = Duration::from_secs(10);
const IDLE_DEFAULT_DURATION: Duration = Duration::from_secs(60);

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
        application_threads: number(args, "--application-threads")
            .map(|v| v as usize)
            .unwrap_or(default.application_threads),
        poll_interval: millis(args, "--poll-interval-ms").unwrap_or(default.poll_interval),
        maintenance_interval: millis(args, "--maintenance-interval-ms")
            .unwrap_or(default.maintenance_interval),
        reconcile_interval: millis(args, "--reconcile-interval-ms")
            .unwrap_or(default.reconcile_interval),
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
            let duration = secs(args, "--duration-secs").unwrap_or(RAMP_DEFAULT_DURATION);
            let grace = secs(args, "--grace-secs").unwrap_or(RAMP_DEFAULT_GRACE);
            let tuning = throughput_tuning(args);

            let probes = runtime().block_on(throughput::run_ramp(&rates, duration, grace, &tuning));
            let ok = report_probes(&probes, "throughput-ramp");
            match probes.iter().rev().find(|p| p.sustained) {
                Some(knee) => eprintln!(
                    "knee: {} rows/sec sustained (achieved {:.0}/sec){}",
                    knee.target_rows_per_sec,
                    knee.achieved_rows_per_sec,
                    match probes.last() {
                        Some(last) if !last.sustained => format!(
                            ", {} rows/sec not (backlog {} after grace)",
                            last.target_rows_per_sec, last.backlog_after_grace
                        ),
                        _ =>
                            ", no tested rate failed — extend --rates to find the knee".to_string(),
                    }
                ),
                None => eprintln!(
                    "no candidate rate sustained — even the lowest tested rate ({} rows/sec) \
                     left a backlog after the grace period",
                    rates.first().copied().unwrap_or(0.0)
                ),
            }
            Some(ok)
        }

        "transaction-shape" => {
            let shapes = usize_list(args, "--shapes", throughput::DEFAULT_SHAPES);
            let target_rate = number(args, "--target-rate").unwrap_or(SHAPE_DEFAULT_TARGET_RATE);
            let duration = secs(args, "--duration-secs").unwrap_or(SHAPE_DEFAULT_DURATION);
            let grace = secs(args, "--grace-secs").unwrap_or(SHAPE_DEFAULT_GRACE);
            let tuning = throughput_tuning(args);

            let probes = runtime().block_on(throughput::run_shape_sweep(
                &shapes,
                target_rate,
                duration,
                grace,
                &tuning,
            ));
            Some(report_probes(&probes, "transaction-shape"))
        }

        "fold-in-ratio" => {
            let ratios = usize_list(args, "--ratios", fold_in::DEFAULT_RATIOS);
            let target_rate = number(args, "--target-rate").unwrap_or(FOLD_IN_DEFAULT_TARGET_RATE);
            let duration = secs(args, "--duration-secs").unwrap_or(FOLD_IN_DEFAULT_DURATION);
            let grace = secs(args, "--grace-secs").unwrap_or(FOLD_IN_DEFAULT_GRACE);
            let tuning = throughput_tuning(args);

            let results = runtime().block_on(fold_in::run_sweep(
                &ratios,
                target_rate,
                duration,
                grace,
                &tuning,
            ));
            let mut ok = true;
            for result in &results {
                println!("{}", result.to_json());
                if result.oracle_ok == Some(false) {
                    eprintln!(
                        "CORRECTNESS FAILURE: fold-in {}:1 — {} of {} groups disagree with the \
                         oracle",
                        result.fold_in_ratio, result.oracle_mismatched_groups, result.oracle_groups
                    );
                    ok = false;
                }
                if result.achieved_rows_per_sec < result.target_rows_per_sec * 0.98 {
                    eprintln!(
                        "fold-in {}:1 — generator offered only {:.0} of {} rows/sec, so this \
                         row is a floor on the engine, not a measurement of it (#276)",
                        result.fold_in_ratio,
                        result.achieved_rows_per_sec,
                        result.target_rows_per_sec
                    );
                }
            }
            Some(ok)
        }

        "intake-ceiling" => {
            let rows_per_commit = number(args, "--rows-per-commit")
                .map(|v| v as usize)
                .unwrap_or(INTAKE_DEFAULT_ROWS_PER_COMMIT);
            let duration = secs(args, "--duration-secs").unwrap_or(INTAKE_DEFAULT_DURATION);
            let grace = secs(args, "--grace-secs").unwrap_or(INTAKE_DEFAULT_GRACE);

            let result = runtime().block_on(intake_ceiling::run(rows_per_commit, duration, grace));
            println!("{}", result.to_json());
            if result.append_backlog > 0 {
                eprintln!(
                    "intake never caught up within the grace period ({} of {} rows appended, \
                     {} behind) — {:.0} rows/sec is a floor, not the ceiling; retry with a \
                     shorter --duration-secs or a longer --grace-secs",
                    result.ring_rows_appended,
                    result.rows_offered,
                    result.append_backlog,
                    result.append_achieved_rows_per_sec
                );
            } else if result.generator_bound {
                eprintln!(
                    "append kept pace with everything offered ({:.0} rows/sec) — the generator \
                     was the limiter, so this is a floor on intake, not its ceiling (#276)",
                    result.append_achieved_rows_per_sec
                );
            }
            Some(true)
        }

        "idle-cost" => {
            let warmup = secs(args, "--warmup-secs").unwrap_or(IDLE_DEFAULT_WARMUP);
            let duration = secs(args, "--duration-secs").unwrap_or(IDLE_DEFAULT_DURATION);
            let tuning = throughput_tuning(args);

            let result = runtime().block_on(idle_cost::run(warmup, duration, &tuning));
            println!("{}", result.to_json());
            eprintln!(
                "idle: {:.1} transactions/sec, {:.0} WAL bytes/sec ({:.2} KB/s), {:.2} seals/sec",
                result.xact_commit_per_sec,
                result.wal_bytes_per_sec,
                result.wal_bytes_per_sec / 1024.0,
                result.seals_per_sec,
            );
            Some(true)
        }

        _ => None,
    }
}

/// Prints one JSON line per probe and flags correctness/cross-check failures.
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
        // A sustained probe drained its whole backlog, so every committed row
        // must have been applied *and* counted: a shortfall there is the
        // #266 metric cross-check failing, not a slow pipeline.
        if probe.sustained && probe.changes_applied < probe.rows_issued {
            eprintln!(
                "HARNESS FAILURE: {} at {} rows/sec reported sustained but applied only {} of \
                 {} committed rows",
                scenario, probe.target_rows_per_sec, probe.changes_applied, probe.rows_issued
            );
            ok = false;
        }
        if probe.achieved_rows_per_sec < probe.target_rows_per_sec * 0.98 {
            eprintln!(
                "{} at {} rows/sec: generator offered only {:.0}/sec — read this row as a \
                 statement about the generator, not the engine (#276)",
                scenario, probe.target_rows_per_sec, probe.achieved_rows_per_sec
            );
        }
    }
    ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::tuning::{
        ISOLATED_PROPAGATION_RECONCILE_INTERVAL, STOCK_MAINTENANCE_INTERVAL, STOCK_POLL_INTERVAL,
        STOCK_RECONCILE_INTERVAL,
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
    fn the_ladder_keeps_the_reconcile_pass_quiet_but_yields_to_the_flag() {
        assert_eq!(
            tuning(&argv(&["hop-ladder"]), EngineTuning::multi_hop()).reconcile_interval,
            ISOLATED_PROPAGATION_RECONCILE_INTERVAL
        );
        assert_eq!(
            tuning(
                &argv(&["hop-ladder", "--reconcile-interval-ms", "5000"]),
                EngineTuning::multi_hop()
            )
            .reconcile_interval,
            STOCK_RECONCILE_INTERVAL
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
    fn an_unknown_name_is_not_handled_here() {
        assert_eq!(run("high-cardinality", &argv(&["high-cardinality"])), None);
    }
}
