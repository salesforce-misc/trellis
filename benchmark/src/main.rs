//! Throughput/latency harness for the `trellis` crate.
//!
//! Issue #63's milestone 0: a deterministic, repeatable backfill benchmark
//! (`posts` -> `posts_calc` -> `posts_totals`) that becomes the yardstick
//! M2/M3/M4 are checked against. See `scenario` for the actual pipeline and
//! measurements, `generate` for the deterministic data shape.
//!
//! ## Running
//!
//! This binary carries `required-features = ["engine-access"]` (ADR-0012; see
//! `Cargo.toml`), so every invocation must pass `--features engine-access` —
//! without it Cargo skips the target and `cargo run` fails outright:
//!
//! ```text
//! cargo run -p benchmark --features engine-access --release -- high-cardinality
//! cargo run -p benchmark --features engine-access --release -- low-cardinality
//! cargo run -p benchmark --features engine-access --release -- both
//! cargo run -p benchmark --features engine-access --release -- custom --n 200000 --g 500 --ceiling-secs 60
//! cargo run -p benchmark --features engine-access --release -- relationship-aggregate
//! ```
//!
//! ## Streaming scenarios
//!
//! The scenarios above time the direct, ring-bypassing backfill build. The
//! streaming ones (see [`streaming`], issue #270) instead drive the real
//! product path through a live [`trellis::Client`] — CDC intake -> ring append
//! -> seal -> claim -> fold -> apply — and are epic #269's validation battery:
//!
//! ```text
//! # V-LAT: the T1 depth ladder, and one depth alone
//! cargo run -p benchmark --features engine-access --release -- hop-ladder
//! cargo run -p benchmark --features engine-access --release -- hop-latency --depth 10 --rate 10 --duration-secs 30
//! cargo run -p benchmark --features engine-access --release -- hop-latency --depth 10 --maintenance-interval-ms 10 --poll-interval-ms 20
//! # V-THRU / V-SHAPE
//! cargo run -p benchmark --features engine-access --release -- throughput-ramp --rates 100000,200000,220000,250000
//! cargo run -p benchmark --features engine-access --release -- transaction-shape --shapes 1,100,10000 --target-rate 20000
//! # V-AGG
//! cargo run -p benchmark --features engine-access --release -- fold-in-ratio --ratios 10,100,1000 --target-rate 400000
//! # issue #277: fold rate and lock-wait attribution by group count x drain workers
//! cargo run -p benchmark --features engine-access --release -- group-contention --groups 10,100,400,4000,40000
//! cargo run -p benchmark --features engine-access --release -- group-contention --groups 400 --threads 1,2,4,8,16
//! # the ceiling every throughput number sits under, and V-IDLE
//! cargo run -p benchmark --features engine-access --release -- intake-ceiling --rows-per-commit 1000 --duration-secs 5
//! cargo run -p benchmark --features engine-access --release -- intake-ceiling --rows-per-commit 1 --rate 300000
//! cargo run -p benchmark --features engine-access --release -- idle-cost --application-threads 8 --duration-secs 60
//! # the load generator's own reach, no engine running (issue #276)
//! cargo run -p benchmark --features engine-access --release -- generator-reach --connections 8 --rows-per-commit 1
//! ```
//!
//! ### Load generators, and the generator-bound flag (issue #276)
//!
//! The latency ladder (`hop-ladder`/`hop-latency`) offers load from one
//! precisely paced connection, unchanged since #270 so its numbers stay
//! comparable. Every other scenario that offers load uses the multi-connection
//! generator ([`streaming::load::run_parallel_load`]): `--connections <n>`
//! writers (default [`streaming::load::DEFAULT_CONNECTIONS`]) paced on one
//! shared schedule at the scenario's target, or flat out for
//! `intake-ceiling`/`generator-reach` (`intake-ceiling --rate` paces it).
//! Every result carrying a target reports `generator_bound`
//! ([`streaming::load::generator_bound`]): true when the achieved rate
//! materially undershot the target while the engine kept up, i.e. the row
//! measured the generator. Read it before reading `kept_target_rate`.
//!
//! ### Verdicts, and what a skipped check reads as
//!
//! - `throughput-ramp`, `transaction-shape`, `fold-in-ratio` and
//!   `group-contention` judge each probe by `kept_target_rate`
//!   ([`streaming::rate`]): the pipeline processed rows at the target rate
//!   (within 2%) *while* load was arriving — a line fitted across the offer
//!   window (`in_window_applied_rows_per_sec` for the 1-1 chain,
//!   `in_window_folded_rows_per_sec` for the aggregate) — **and** its backlog
//!   drained within the grace period (`drained`). `drained` alone is not a
//!   verdict: a pipeline at half the target still drains a 20s window inside
//!   a 30s grace (issue #319). The ramp's knee is the highest rate that kept
//!   its target, and the ramp stops at the first that didn't.
//! - `hop-ladder`'s T1 flags (`t1_*_pass`, `e2e_max_under_1s`) count rows
//!   that never reached the terminal hop within the drain grace as misses
//!   above every bound, rather than judging only the rows that landed.
//! - A check that didn't run reads as `null`, never as a passing value:
//!   `fold-in-ratio`'s aggregate oracle is skipped when the target never
//!   drained (it would hold partial sums), and then both `oracle_ok` and
//!   `oracle_mismatched_groups` are `null` (issue #335); an unfittable
//!   in-window rate is `null` and fails `kept_target_rate`.
//! - The 1-1 chain's oracle (`oracle_ok`) always runs, over the rows that
//!   landed: it is "correct as far as it got", with `oracle_fully_converged`
//!   saying whether that was everything.
//!
//! All the engine scenarios accept `--application-threads`, `--poll-interval-ms`,
//! `--maintenance-interval-ms`, `--reconcile-interval-ms` and
//! `--group-commit <max_rows>,<max_delay_ms>|off` (issue #274; the last
//! defaults to stock `ClientOptions::default()`'s shipped-on group-commit,
//! `off` measures the un-grouped escape hatch) —
//! [`streaming::tuning::EngineTuning`], which is also where a later child of
//! #269 adds a knob of its own. `intake-ceiling` takes its own
//! `--group-commit` directly (it is deliberately not built from
//! `EngineTuning` — see [`streaming::intake_ceiling`]'s module doc comment).
//! Each prints one JSON line per measurement
//! point, cross-checks `trellis_changes_applied_total` against the rows its
//! generator committed, and runs an independent SQL oracle over the terminal
//! target — the process exits non-zero on an oracle or cross-check failure,
//! but **not** on a missed latency target or a rate the engine couldn't
//! keep, which are measurements rather than errors.
//!
//! `--release` matters: this pushes 1M rows through a real Postgres
//! instance and a real CDC pipeline, and the debug-build overhead is large
//! enough to distort the numbers. Each scenario prints one line of JSON to
//! stdout (see [`scenario::BenchResult::to_json`]) and the process exits
//! non-zero if the aggregate-backfill phase exceeds its regression
//! ceiling — wire this into CI as `cargo run -p benchmark --features
//! engine-access --release -- high-cardinality`.

mod generate;
mod scenario;
mod scenario_relationship;
mod streaming;

use std::time::Duration;

/// Post-M3 this shape's aggregate phase measures ~0.7-0.8s on this
/// harness/box across repeated runs — the direct, single-pass-then-chunked
/// build ([`trellis::dev::defs::backfill_definition`], issue #63) replaced the ring
/// drain that took ~55-58s here (and ~1m50s on the issue's poc cluster). The
/// M3-review fix (aggregate the source once into a staging table, then chunk
/// the writes from that small table instead of re-scanning the source per
/// chunk) took this from ~1.25s to ~0.75s. 10s keeps >10x headroom over this
/// box's measurement for CI/dev-machine jitter and cold caches while still
/// firing long before any regression back toward the old tens-of-seconds
/// mechanism. Revisit once a CI-hardware baseline exists.
const HIGH_CARDINALITY_CEILING: Duration = Duration::from_secs(10);

/// Post-M3 this shape's aggregate phase measures ~35ms on this harness/box:
/// with only 100 groups the direct build issues a single group-key chunk, so
/// it's far faster than the 100k-group high-cardinality shape (which they no
/// longer track — the direct build's cost scales with group count, so the two
/// diverge sharply, as M3 intended). 5s keeps generous headroom while still
/// catching a regression that would make small-group builds pathological.
const LOW_CARDINALITY_CEILING: Duration = Duration::from_secs(5);

struct Scenario {
    name: &'static str,
    n: i64,
    g: i64,
    ceiling: Duration,
}

const HIGH_CARDINALITY: Scenario = Scenario {
    name: "high-cardinality",
    n: 1_000_000,
    g: 100_000,
    ceiling: HIGH_CARDINALITY_CEILING,
};

const LOW_CARDINALITY: Scenario = Scenario {
    name: "low-cardinality",
    n: 1_000_000,
    g: 100,
    ceiling: LOW_CARDINALITY_CEILING,
};

/// Row counts for the relationship-aggregate scenario (issue #63, C3),
/// matching the real-world ratios reported in the poc that motivated
/// `backfill_relationship_one_to_one`: 100k authors, 1M posts, 4.5M comments.
const RELATIONSHIP_AUTHORS: i64 = 100_000;
const RELATIONSHIP_POSTS: i64 = 1_000_000;
const RELATIONSHIP_COMMENTS: i64 = 4_500_000;

/// Measured ~660-670ms for `install_definition` end to end (target-table
/// creation + the direct relationship-aware build,
/// `backfill_relationship_one_to_one`) on this harness/box across repeated
/// runs against the full 100k/1M/4.5M row counts above — down from the ~1
/// minute the ring path took on the real-world shape that motivated this
/// benchmark (issue #63 C2's handoff doc). 10s keeps >10x headroom for
/// CI/dev-machine jitter and cold caches while still firing long before any
/// regression back toward the ring's tens-of-seconds mechanism. Revisit once
/// a CI-hardware baseline exists.
const RELATIONSHIP_AGGREGATE_CEILING: Duration = Duration::from_secs(10);

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let name = args.first().map(String::as_str).unwrap_or("both");

    if let Some(ok) = streaming::cli::run(name, &args) {
        if !ok {
            std::process::exit(1);
        }
        return;
    }

    if name == "relationship-aggregate" {
        let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");
        let result = runtime.block_on(scenario_relationship::run(
            "relationship-aggregate",
            RELATIONSHIP_AUTHORS,
            RELATIONSHIP_POSTS,
            RELATIONSHIP_COMMENTS,
            RELATIONSHIP_AGGREGATE_CEILING,
        ));
        println!("{}", result.to_json());
        let mut failed = false;
        if !result.within_ceiling {
            failed = true;
            eprintln!(
                "REGRESSION: {} install_definition took {}ms, over its {}ms ceiling",
                result.scenario, result.backfill_ms, result.ceiling_ms
            );
        }
        if !result.correctness_ok {
            failed = true;
            eprintln!(
                "CORRECTNESS FAILURE: {}'s backfilled author_totals did not match the oracle",
                result.scenario
            );
        }
        if failed {
            std::process::exit(1);
        }
        return;
    }

    let scenarios = match parse_args(&args, name) {
        Ok(scenarios) => scenarios,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };

    let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");
    let mut any_over_ceiling = false;
    let mut any_incorrect = false;

    for scenario in scenarios {
        let result = runtime.block_on(scenario::run(
            scenario.name,
            scenario.n,
            scenario.g,
            scenario.ceiling,
        ));
        println!("{}", result.to_json());
        if !result.within_ceiling {
            any_over_ceiling = true;
            eprintln!(
                "REGRESSION: {} aggregate backfill took {}ms, over its {}ms ceiling",
                result.scenario, result.aggregate_backfill_ms, result.ceiling_ms
            );
        }
        if !result.correctness_ok {
            any_incorrect = true;
            eprintln!(
                "CORRECTNESS FAILURE: {}'s backfilled posts_totals did not match the oracle",
                result.scenario
            );
        }
    }

    if any_over_ceiling || any_incorrect {
        std::process::exit(1);
    }
}

/// Parses argv into the list of scenarios to run. `custom` reads
/// `--n`/`--g`/`--ceiling-secs` (all required); every other name is one of
/// the two fixed scenarios above, or `both` for both of them in sequence.
/// (`relationship-aggregate` is handled separately in `main` — it doesn't fit
/// this `n`/`g` shape.)
fn parse_args(args: &[String], name: &str) -> Result<Vec<Scenario>, String> {
    match name {
        "high-cardinality" => Ok(vec![HIGH_CARDINALITY]),
        "low-cardinality" => Ok(vec![LOW_CARDINALITY]),
        "both" => Ok(vec![HIGH_CARDINALITY, LOW_CARDINALITY]),
        "custom" => {
            let n = parse_flag(args, "--n")?;
            let g = parse_flag(args, "--g")?;
            let ceiling_secs = parse_flag(args, "--ceiling-secs")?;
            Ok(vec![Scenario {
                name: "custom",
                n,
                g,
                ceiling: Duration::from_secs(ceiling_secs as u64),
            }])
        }
        other => Err(format!(
            "unknown scenario {other:?} — expected one of: high-cardinality, low-cardinality, \
             both, relationship-aggregate, custom, {}",
            streaming::cli::SCENARIOS.join(", ")
        )),
    }
}

fn parse_flag(args: &[String], flag: &str) -> Result<i64, String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .ok_or_else(|| format!("custom scenario requires {flag} <value>"))?
        .parse::<i64>()
        .map_err(|e| format!("{flag} must be an integer: {e}"))
}
