//! Steady-state streaming benchmark harness.
//!
//! Everything here drives the real, product-facing streaming path: a live [`trellis::Client`] doing trigger
//! capture into the ring -> seal -> claim -> fold -> apply, fed by a
//! controlled-rate load generator, measured through the Prometheus
//! histograms that already exist
//! ([`trellis::Metrics::render_prometheus`]). No new instrumentation: the
//! latency targets are stated at exactly
//! [`trellis::metrics::LATENCY_BUCKETS`]' own boundaries so a
//! cumulative-bucket-fraction read off a scrape answers them with no
//! interpolation and no estimator.
//!
//! Every scenario here
//!
//! 1. builds its topology through the real front door
//!    ([`trellis::dev::defs::install_definition`]), not a shortcut,
//! 2. waits for it to be `live` *and* demonstrably flowing end to end
//!    before its measurement window opens ([`chain::warm_up`]),
//! 3. cross-checks `trellis_changes_applied_total` against the rows the
//!    generator actually committed, so a latency number that looks fast
//!    because most changes were never observed gets caught,
//! 4. runs an independent SQL oracle over the terminal target, so a
//!    fast-but-wrong result fails, and
//! 5. prints exactly one line of JSON, following
//!    [`crate::scenario::BenchResult::to_json`]'s hand-rolled convention.
//!
//! Submodules:
//!
//! - [`cli`]: flag parsing and dispatch for the scenarios below.
//! - [`tuning`]: the engine knobs a scenario varies ([`tuning::EngineTuning`]),
//!   and the single place they're mapped onto [`trellis::ClientOptions`].
//!   This is the documented extension point for later scenarios.
//! - [`chain`]: the N-hop 1-1 chain builder, plus liveness/warm-up/oracle.
//! - [`load`]: the single-connection paced generator, the multi-connection
//!   paced-or-max-rate one, and the generator-bound self-check.
//! - [`generator_reach`]: the multi-connection generator alone, no engine —
//!   how much load the instrument itself can offer.
//! - [`scrape`]: `render_prometheus()` readers — T1's bucket-fraction
//!   convention, exact per-hop means, plain counters.
//! - [`rate`]: the in-window rate fit and the `kept_target_rate` verdict the
//!   throughput and fold-in scenarios share.
//! - [`hop_latency`]: the hop-depth latency ladder (V-LAT / T1).
//! - [`throughput`]: the single-hop probe, the throughput ramp (V-THRU / T2)
//!   and the transaction-shape sweep (V-SHAPE).
//! - [`fold_in`]: the aggregate fold-in-ratio sweep (V-AGG / T3), and a
//!   group-count x drain-worker contention grid over the same probe.
//! - [`contention`]: `pg_stat_activity` sampling that attributes engine
//!   backend time to row-lock waits vs everything else, and per-episode
//!   lock wait and page lock hold times by statement class.
//! - [`write_tax`]: what capturing a table costs the application writing to
//!   it, per capture variant and transaction shape.
//! - [`capture_ceiling`]: rows/s reaching the ring as writers are added,
//!   with nothing draining. It replaced `intake-ceiling`.
//! - [`ssi_tax`]: the `40001` rate capture adds to concurrent
//!   `SERIALIZABLE` writers, against the same table with no capture.
//! - [`join`]: the `JOIN … GROUP BY` scenario (D1/D2 x S1/S2,
//!   a-side and b-side pages, hot keys) against a per-fact control.
//! - [`idle_cost`]: a zero-traffic install's transactions/sec, WAL bytes/sec
//!   and seals/sec (V-IDLE).
//! - [`build_under_load`]: a large aggregate build under a paced mixed write
//!   load, checked against a SQL oracle, with peak RSS per phase.
//! - [`disk_tier`]: WAL/fsync/checkpoint deltas over a window, the cluster's
//!   storage, and a fixed-size commit-latency histogram.
//! - [`process_memory`]: in-process `VmRSS` sampling (peak, per phase,
//!   `VmHWM`, the cgroup's `memory.peak`).
//! - [`server_cost`]: Postgres CPU, `deadlock detected` log lines, the
//!   engine's lock-timeout warnings and ledger bytes over a window.

pub mod apply_latency;
pub mod build_profile;
pub mod build_under_load;
pub mod capture_ceiling;
pub mod chain;
pub mod cli;
pub mod contention;
pub mod disk_tier;
pub mod fold_in;
pub mod generator_reach;
pub mod hop_latency;
pub mod idle_cost;
pub mod join;
pub mod load;
pub mod process_memory;
pub mod rate;
pub mod scrape;
pub mod self_check_walk;
pub mod server_cost;
pub mod ssi_tax;
pub mod throughput;
pub mod tuning;
pub mod write_tax;
