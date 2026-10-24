//! Throughput/latency harness for the `trellis` crate.
//!
//! ## Running
//!
//! This binary carries `required-features = ["engine-access"]` (ADR-0012; see
//! `Cargo.toml`), so every invocation must pass `--features engine-access` —
//! without it Cargo skips the target and `cargo run` fails outright.
//!
//! ## Scenarios
//!
//! The scenarios (see [`streaming`], issue #270) drive the real product path
//! through a live [`trellis::Client`] — trigger capture into the ring ->
//! seal -> claim -> fold -> apply — and are epic #269's validation battery:
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
//! # #1113: #1091 J2's JOIN scenario (D1/D2 x S1/S2, a-side and b-side pages, hot keys)
//! # against its same-session per-fact control (B0)
//! cargo run -p benchmark --features engine-access --release -- join
//! cargo run -p benchmark --features engine-access --release -- join --datasets d1 --facts 1000000 --hot-pages 0
//! # the write-path tax and the capture ceiling (#622 C4; #565 E1/E2), and V-IDLE
//! cargo run -p benchmark --features engine-access --release -- write-tax --shapes 1x1,1000x16,orm100x1 --reps 3
//! cargo run -p benchmark --features engine-access --release -- write-tax --variants none,trigger --shapes orm100x16+hold
//! cargo run -p benchmark --features engine-access --release -- capture-ceiling --writers 1,4,8,16,32
//! cargo run -p benchmark --features engine-access --release -- idle-cost --application-threads 8 --duration-secs 60
//! # per-statement attribution (issue #350): the cluster must preload pg_stat_statements
//! TRELLIS_TESTKIT_PG_OPTIONS='shared_preload_libraries=pg_stat_statements pg_stat_statements.track=all track_activity_query_size=4096' \
//!   cargo run -p benchmark --features engine-access --release -- idle-cost --statements [--no-staging-worker | --application-threads 0]
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
//! `generator-reach`. `write-tax` and `capture-ceiling` have writers of their
//! own ([`streaming::write_tax`]): a fixed number of transactions per writer
//! shape, cut off at `--max-secs`.
//! Every result carrying a target reports `generator_bound`
//! ([`streaming::load::generator_bound`]): true when the achieved rate
//! materially undershot the target while the engine kept up, i.e. the row
//! measured the generator. Read it before reading `kept_target_rate`.
//!
//! ### Verdicts, and what a skipped check reads as
//!
//! - `throughput-ramp`, `transaction-shape`, `fold-in-ratio` and
//!   `group-contention` judge each probe by `kept_target_rate`
//!   ([`streaming::rate`]): the pipeline processed rows at the offered rate
//!   *while* load was arriving — a line fitted across the offer window
//!   (`in_window_applied_rows_per_sec` for the 1-1 chain,
//!   `in_window_folded_rows_per_sec` for the aggregate) within
//!   `rate_tolerance` (2%, widened for commits too coarse for the window to
//!   fit exactly) — **and** its backlog drained within the grace period
//!   (`drained`). The offered rate is the target, or the achieved rate when
//!   the generator undershot, which `generator_bound` then flags. `drained`
//!   alone is not a verdict: a pipeline at half the target still drains a
//!   20s window inside a 30s grace (issue #319). The ramp's knee is the
//!   highest rate that kept its target, and the ramp stops at the first that
//!   didn't. A probe whose applied-changes counter disagrees with the rows
//!   it committed (issues #266, #423) fails the run and reads
//!   `kept_target_rate: false`, but it measured nothing, so the ramp steps
//!   past it rather than stopping (issue #509).
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
//! `--maintenance-interval-ms`, `--reconcile-interval-ms` and `--drain-batch-cap`
//! (issue #620: the most folded records one drain batch holds; a small value
//! forces paging, and `TRELLIS_BENCH_LOG=trellis::staging::apply=info` logs
//! each paged drain's page count) —
//! [`streaming::tuning::EngineTuning`], which is also where a later child of
//! #269 adds a knob of its own. `write-tax` and `capture-ceiling` take
//! none of them: they run no client, and nothing drains in either.
//! Each prints one JSON line per measurement
//! point, cross-checks `trellis_changes_applied_total` against the rows its
//! generator committed, and runs an independent SQL oracle over the terminal
//! target — the process exits non-zero on an oracle or cross-check failure,
//! but **not** on a missed latency target or a rate the engine couldn't
//! keep, which are measurements rather than errors.
//!
//! `TRELLIS_BENCH_HOUSEKEEPING_MS=<ms>` sets the housekeeping interval (stock
//! 5 s) for a run: a hot run's convergence tail is quantised by that
//! interval, which biases its changes/s. A value that is not a whole number
//! of milliseconds fails the run.
//!
//! `--release` matters: this pushes 1M rows through a real Postgres
//! instance and a real CDC pipeline, and the debug-build overhead is large
//! enough to distort the numbers. Each scenario prints one line of JSON per
//! measurement point to stdout.

mod scenario;
mod streaming;

fn main() {
    // Opt-in engine logs for debugging a probe: TRELLIS_BENCH_LOG=warn (or a
    // full EnvFilter directive) writes tracing output to stderr. The engine
    // warning counter (`lock_timeout_warnings` in the scenarios' JSON) is
    // always on.
    {
        use tracing_subscriber::prelude::*;
        let stderr = std::env::var("TRELLIS_BENCH_LOG").ok().map(|filter| {
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(tracing_subscriber::EnvFilter::new(filter))
        });
        tracing_subscriber::registry()
            .with(streaming::scrape::engine_log_counter())
            .with(stderr)
            .init();
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    let name = args.first().map(String::as_str).unwrap_or("");

    if let Some(ok) = streaming::cli::run(name, &args) {
        if !ok {
            std::process::exit(1);
        }
        return;
    }

    eprintln!(
        "unknown scenario {name:?}: expected one of: {}",
        streaming::cli::SCENARIOS.join(", ")
    );
    std::process::exit(2);
}
