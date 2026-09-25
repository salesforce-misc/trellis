//! The engine knobs the streaming scenarios vary, and the one place they're
//! mapped onto [`ClientOptions`].
//!
//! Every field here is an existing [`ClientOptions`] field — this harness
//! measures `main` as it is, and deliberately carries no knob the engine
//! doesn't already have.
//!
//! ## Extension point (epic #269)
//!
//! Later children of #269 add engine options of their own (a seal mode, an
//! intake group-commit config). Wiring one up is meant to be three edits and
//! no new plumbing:
//!
//! 1. add the field to [`EngineTuning`] (with its stock value in
//!    [`EngineTuning::default`], so every existing scenario keeps measuring
//!    stock behavior),
//! 2. set the corresponding [`ClientOptions`] field in
//!    [`EngineTuning::client_options`],
//! 3. read a CLI flag for it in [`super::cli`]'s `tuning`, which is the single
//!    function every scenario's flags go through.
//!
//! Scenarios themselves take `&EngineTuning` and never touch
//! [`ClientOptions`] directly, so none of them need editing to gain a knob —
//! which is the point: a later child measuring its own engine change against
//! this baseline should not be re-plumbing the harness.

use std::time::Duration;

use trellis::ClientOptions;

/// [`ClientOptions::poll_interval`]'s own stock default (200 ms) — the idle
/// `LISTEN` backstop under `NOTIFY` delivery.
pub const STOCK_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// [`ClientOptions::maintenance_interval`]'s own stock default (300 ms) — the
/// seal cadence, and the only place a sealed (claimable) batch is ever
/// created. Issue #266's H1.
pub const STOCK_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(300);

/// [`ClientOptions::reconcile_interval`]'s own stock default (5 s), and what
/// the single-hop scenarios run at: they have no intermediate hops for the
/// reconcile pass to publish, so the stock value is the honest one.
///
/// It used to matter to them in one way: the reconcile pass discharged the
/// catch-up backfill a definition parked when it went live, and that
/// discharge re-stages every row the source holds by then (#423). Since #476
/// a definition reports live only once that catch-up has been discharged,
/// and the maintenance loop discharges a freshly parked marker on its next
/// tick, so the probes' wait for it (`chain::wait_for_catch_up_discharged`)
/// no longer costs setup a reconcile interval.
pub const STOCK_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);

/// One scenario's engine configuration. [`Default`] is stock `main` in every
/// field, so a scenario that overrides nothing measures the shipped defaults.
#[derive(Debug, Clone)]
pub struct EngineTuning {
    /// How many drain workers the client runs
    /// ([`ClientOptions::application_threads`]).
    pub application_threads: usize,
    /// The idle-wake backstop ([`ClientOptions::poll_interval`]).
    pub poll_interval: Duration,
    /// The seal cadence ([`ClientOptions::maintenance_interval`]).
    pub maintenance_interval: Duration,
    /// How often the publication/backfill reconcile pass runs
    /// ([`ClientOptions::reconcile_interval`]) — see
    /// [`STOCK_RECONCILE_INTERVAL`] for why this is a knob at all.
    pub reconcile_interval: Duration,
    /// Intake's group-commit batching bounds
    /// ([`ClientOptions::group_commit`], issue #274). `Some` (matching
    /// `ClientOptions::default()`) is stock `main`'s shipped, default-on
    /// behavior; `None` measures the un-grouped escape hatch instead.
    pub group_commit: Option<trellis::GroupCommitConfig>,
}

impl Default for EngineTuning {
    fn default() -> Self {
        Self {
            // Not `ClientOptions::default()`'s 0: every streaming scenario
            // needs at least one drain worker to make progress at all, and
            // `install_definition`'s Backfilling -> Live flip needs one even
            // for an empty chunk queue (see `chain::install_chain_hops`).
            // 4 is what the reference harness's latency ladder measured
            // with; the throughput scenarios override it to 8
            // (`SEG_BUCKETS`' worth of claim parallelism per batch).
            application_threads: 4,
            poll_interval: STOCK_POLL_INTERVAL,
            maintenance_interval: STOCK_MAINTENANCE_INTERVAL,
            reconcile_interval: STOCK_RECONCILE_INTERVAL,
            group_commit: Some(trellis::GroupCommitConfig::default()),
        }
    }
}

impl EngineTuning {
    /// The multi-hop latency ladder's defaults: stock in every field.
    ///
    /// The ladder used to run with an hour-long reconcile interval, so the
    /// reconcile pass never fired and intermediate hop tables never joined
    /// the CDC publication, where each of their writes was staged twice
    /// (once by apply's in-transaction `Recompute`, again by CDC decoding).
    /// #315 keeps every definition's target out of the publication, so that
    /// reason is gone (#471). And the reconcile pass is what parks a newly
    /// registered hop's backfill marker, so an hour-long interval kept every
    /// hop after the first `waiting_to_backfill` for an hour.
    pub fn multi_hop() -> Self {
        Self::default()
    }

    /// This tuning as [`ClientOptions`], for a staging-worker client
    /// publishing `source_tables`.
    pub fn client_options(&self, source_tables: Vec<String>) -> ClientOptions {
        ClientOptions {
            staging_worker: true,
            application_threads: self.application_threads,
            source_tables,
            poll_interval: self.poll_interval,
            maintenance_interval: self.maintenance_interval,
            reconcile_interval: self.reconcile_interval,
            group_commit: self.group_commit,
            ..Default::default()
        }
    }
}
