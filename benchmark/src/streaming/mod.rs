//! Steady-state streaming benchmark harness (issue #270, ported from the
//! throwaway `performance-investigation` branch that issues #266/#268 were
//! measured on).
//!
//! Unlike [`crate::scenario`]/[`crate::scenario_relationship`], which time
//! [`trellis::dev::defs::backfill_definition`] — the direct, ring-bypassing
//! set-based build (ADR-0007) — everything here drives the real,
//! product-facing streaming path: a live [`trellis::Client`] doing CDC
//! intake -> ring append -> seal -> claim -> fold -> apply, fed by a
//! controlled-rate load generator, measured through the Prometheus
//! histograms that already exist
//! ([`trellis::Metrics::render_prometheus`]). No new instrumentation: issue
//! #266 states the latency targets at exactly
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
//!   This is the documented extension point for epic #269's later children.
//! - [`chain`]: the N-hop 1-1 chain builder, plus liveness/warm-up/oracle.
//! - [`load`]: the single-connection paced generator, the multi-connection
//!   paced-or-max-rate one, and the generator-bound self-check (#276).
//! - [`generator_reach`]: the multi-connection generator alone, no engine —
//!   how much load the instrument itself can offer.
//! - [`scrape`]: `render_prometheus()` readers — T1's bucket-fraction
//!   convention, exact per-hop means, plain counters.
//! - [`rate`]: the in-window rate fit and the `kept_target_rate` verdict the
//!   throughput and fold-in scenarios share (#276, #319).
//! - [`hop_latency`]: the hop-depth latency ladder (V-LAT / T1).
//! - [`throughput`]: the single-hop probe, the throughput ramp (V-THRU / T2)
//!   and the transaction-shape sweep (V-SHAPE).
//! - [`fold_in`]: the aggregate fold-in-ratio sweep (V-AGG / T3), and issue
//!   #277's group-count x drain-worker contention grid over the same probe.
//! - [`contention`]: `pg_stat_activity` sampling that attributes engine
//!   backend time to row-lock waits vs everything else (#277).
//! - [`intake_ceiling`]: CDC decode + ring append alone, the ceiling every
//!   other throughput number sits under.
//! - [`idle_cost`]: a zero-traffic install's transactions/sec, WAL bytes/sec
//!   and seals/sec (V-IDLE).

pub mod chain;
pub mod cli;
pub mod contention;
pub mod fold_in;
pub mod generator_reach;
pub mod hop_latency;
pub mod idle_cost;
pub mod intake_ceiling;
pub mod load;
pub mod rate;
pub mod scrape;
pub mod throughput;
pub mod tuning;
